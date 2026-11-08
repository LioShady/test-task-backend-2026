import logging
from datetime import datetime
from typing import Dict, Iterable, Iterator, List, Tuple

from sqlalchemy import func, select, update

from abstract_customer import (
    company_row, contact_row, customer_login, customer_row,
    dedup_by_id, delete_abstract_customers,
    fetch_abstract_customer_ids_by_customer_login,
    fetch_abstract_customer_ids_by_lead_login,
    fetch_amo_customers_by_ids, fetch_amo_leads_by_ids,
    insert_abstract_customers, lead_login, lead_row,
    reassign_entities_to_customer,
    upsert_companies, upsert_contacts, upsert_customers, upsert_leads,
)
from amocrm import COMPANIES, CONTACTS, Customer, Lead, amocrm_get
from models import (AbstractCustomer, AmoCompany, AmoContact, AmoCustomer,
                    AmoLead, db)

logger = logging.getLogger(__name__)

BATCH_SIZE = 2000


# --- утилиты ----------------------------------------------------------------

def _chunks(it: Iterable, size: int) -> Iterator[List]:
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def _now() -> datetime:
    return datetime.utcnow()


def _mark_synced(model, ids: List[int], run_start: datetime):
    if not ids:
        return
    db.session.execute(
        update(model.__table__)
        .where(model.id.in_(ids))
        .values(synced_at=run_start)
    )


def _collect_customers_batch(batch: List[Customer], run_start: datetime,
                             is_full: bool) -> None:
    """Обрабатывает пачку покупателей: resolve -> insert новых склеек ->
    upsert amo_customer -> mark synced."""
    # 1. Отбрасываем покупателей без логина и (в неполном режиме) по статусу.
    relevant = []
    for c in batch:
        if not is_full and c.status_id == 111349:
            continue
        if not customer_login(c):
            continue
        relevant.append(c)
    if not relevant:
        return

    crm_ids = [c.id for c in relevant]
    logins = list({customer_login(c) for c in relevant})

    # Существующие amo_customer по id -> их abstract_customer_id.
    existing_by_id = fetch_amo_customers_by_ids(crm_ids)
    # Существующие склейки по логину среди сделок (
    by_lead_login = fetch_abstract_customer_ids_by_lead_login(logins)
    by_customer_login = fetch_abstract_customer_ids_by_customer_login(logins)

    resolved: Dict[int, int] = {}  # crm_id -> abstract_customer_id

    for c in relevant:
        login = customer_login(c)
        existing = existing_by_id.get(c.id)
        if existing is not None:
            resolved[c.id] = existing.abstract_customer_id
            continue

        candidates = set(by_lead_login.get(login, [])) | set(by_customer_login.get(login, []))
        if len(candidates) > 1:
            # Если по логину нашлось больше одной записи, то совпадение не используется.
            logger.warning("Customer %s login %s: more than one abstract_customer %s - create new",
                           c.id, login, candidates)
            continue
        if len(candidates) == 1:
            resolved[c.id] = next(iter(candidates))

    # Создаём новые склейки пачкой (RETURNING id) и раздаём id покупателям.
    new_order: List[int] = []  # crm_id в порядке VALUES
    new_rows: List[Dict] = []
    for c in relevant:
        if c.id in resolved:
            continue
        new_order.append(c.id)
        new_rows.append({'send_methods': None})
    if new_rows:
        inserted_ids = insert_abstract_customers(new_rows)
        if len(inserted_ids) != len(new_order):
            raise RuntimeError(
                f"insert_abstract_customers returned {len(inserted_ids)} ids "
                f"for {len(new_order)} rows -- cannot map reliably"
            )
        for crm_id, ac_id in zip(new_order, inserted_ids):
            resolved[crm_id] = ac_id

    # Готовим строки amo_customer и вложенные сущности.
    customer_rows = [customer_row(c, resolved[c.id]) for c in relevant]

    contact_rows: List[Dict] = []
    company_rows: List[Dict] = []
    for c in relevant:
        ac_id = resolved[c.id]
        for contact in (c.contacts or []):
            if contact.get('id') is None:
                continue
            contact_rows.append(contact_row(contact, ac_id))
        for company in (c.companies or []):
            if company.get('id') is None:
                continue
            company_rows.append(company_row(company, ac_id))

    # Bulk upsert + отметка synced_at
    upsert_customers(customer_rows)
    upsert_contacts(contact_rows)
    upsert_companies(company_rows)

    _mark_synced(AmoCustomer, [c.id for c in relevant], run_start)
    if contact_rows:
        _mark_synced(AmoContact, [r['id'] for r in dedup_by_id(contact_rows)], run_start)
    if company_rows:
        _mark_synced(AmoCompany, [r['id'] for r in dedup_by_id(company_rows)], run_start)



def _collect_leads_batch(batch: List[Lead], run_start: datetime,
                         is_full: bool) -> None:
    relevant = []
    for l in batch:
        if not is_full and l.status_id == 143:
            continue
        relevant.append(l)
    if not relevant:
        return

    crm_ids = [l.id for l in relevant]
    logins = list({lead_login(l) for l in relevant if lead_login(l)})

    existing_by_id = fetch_amo_leads_by_ids(crm_ids)
    by_customer_login = fetch_abstract_customer_ids_by_customer_login(logins)

    resolved: Dict[int, int] = {}       # lead crm_id -> abstract_customer_id
    merges: List[Tuple[int, int]] = []  # (source_ac_id, target_ac_id)
    new_leads: List[Lead] = []

    for l in relevant:
        login = lead_login(l)
        existing = existing_by_id.get(l.id)
        if existing is not None:
            target = existing.abstract_customer_id
            resolved[l.id] = target
            # Если сделка нашлась по id и есть покупатель с тем же
            # логином в другой склейке, сливаем в ту, что нашли по сделке
            # вторая удаляется.
            if login:
                candidates = by_customer_login.get(login, [])
                if len(candidates) > 1:
                    logger.warning("Lead %s login %s: more than one abstract_customer %s",
                                   l.id, login, candidates)
                elif len(candidates) == 1 and candidates[0] != target:
                    merges.append((candidates[0], target))
            continue

        # по id не нашли - ищем по логину среди покупателей
        if login:
            candidates = by_customer_login.get(login, [])
            if len(candidates) == 1:
                resolved[l.id] = candidates[0]
                continue
            if len(candidates) > 1:
                logger.warning("Lead %s login %s: more than one abstract_customer %s -- create new",
                               l.id, login, candidates)
        # ничего не нашли - создаём новую склейку
        new_leads.append(l)

    # Создаём склейки под новые сделки.
    if new_leads:
        new_rows = [{'send_methods': None} for _ in new_leads]
        inserted_ids = insert_abstract_customers(new_rows)
        if len(inserted_ids) != len(new_leads):
            raise RuntimeError("insert_abstract_customers: cannot map ids for leads")
        for l, ac_id in zip(new_leads, inserted_ids):
            resolved[l.id] = ac_id

    # Слияния: переносим сущности из source на target, source удаляем.
    merge_pairs = {(s, t) for s, t in merges if s != t}
    if merge_pairs:
        by_source: Dict[int, int] = {}
        for s, t in merge_pairs:
            if s not in by_source:
                by_source[s] = t
            elif by_source[s] != t:
                logger.warning("Merge conflict for source %s: targets %s and %s, use %s",
                               s, by_source[s], t, by_source[s])
        for source, target in by_source.items():
            reassign_entities_to_customer([source], target)
            for lead_id, ac_id in list(resolved.items()):
                if ac_id == source:
                    resolved[lead_id] = target
        delete_abstract_customers(list(by_source.keys()))

    # Готовим строки amo_lead и вложенные контакты.
    lead_rows = [lead_row(l, resolved[l.id]) for l in relevant]
    contact_rows: List[Dict] = []
    for l in relevant:
        ac_id = resolved[l.id]
        for contact in (l.contacts or []):
            if contact.get('id') is None:
                continue
            contact_rows.append(contact_row(contact, ac_id))

    upsert_leads(lead_rows)
    upsert_contacts(contact_rows)

    _mark_synced(AmoLead, [l.id for l in relevant], run_start)
    if contact_rows:
        _mark_synced(AmoContact, [r['id'] for r in dedup_by_id(contact_rows)], run_start)



def _cleanup(run_start: datetime):
    """Удаляем всё, что не было в текущей выгрузке (synced_at < run_start),
    плюс сущности с «удалить» в имени, плюс пустые склейки.
    """
    logger.info("Cleanup: stale entities (synced_at < %s)", run_start)


    for model in (AmoContact, AmoCompany, AmoLead, AmoCustomer):
        deleted = db.session.execute(
            model.__table__.delete().where(model.synced_at < run_start)
        ).rowcount
        logger.info("  %s: deleted %s stale", model.__tablename__, deleted)

    for model in (AmoLead, AmoCustomer, AmoContact, AmoCompany):
        deleted = db.session.execute(
            model.__table__.delete().where(
                func.coalesce(func.lower(model.name), '').contains('удалить')
            )
        ).rowcount
        logger.info("  %s: deleted %s with 'удалить' in name", model.__tablename__, deleted)

    # Склейки без каких-либо сущностей.
    orphan_ac = db.session.execute(
        select(AbstractCustomer.id).where(
            ~AbstractCustomer.customers.any(),
            ~AbstractCustomer.leads.any(),
            ~AbstractCustomer.contacts.any(),
            ~AbstractCustomer.companies.any(),
        )
    ).scalars().all()
    if orphan_ac:
        db.session.execute(
            AbstractCustomer.__table__.delete().where(AbstractCustomer.id.in_(orphan_ac))
        )
        logger.info("  abstract_customer: deleted %s empty", len(orphan_ac))



def collect_leads_customers(is_full: bool = True):
    """Ночная синхронизация CRM -> Postgres.

    Полный проход (is_full=True):
      1) покупатели батчами -> upsert amo_customer + контакты/компании;
      2) сделки батчами -> resolve/merge -> upsert amo_lead + контакты;
      3) удаление всего, чего не было в выгрузке (по synced_at) + «удалить»
         в имени + пустые склейки.
    Если сбор прервался исключением -- удаление не запускается.
    """
    run_start = _now()

    try:
        logger.info("Collect customers is_full=%s", is_full)
        for batch in _chunks(amocrm_get(Customer, _with=f"{COMPANIES},{CONTACTS}"),
                             BATCH_SIZE):
            _collect_customers_batch(batch, run_start, is_full)
            db.session.commit()

        logger.info("Collect leads is_full=%s", is_full)
        for batch in _chunks(amocrm_get(Lead, _with=f"{CONTACTS}"), BATCH_SIZE):
            _collect_leads_batch(batch, run_start, is_full)
            db.session.commit()
    except Exception:
        logger.exception("Collection failed -- rollback, cleanup skipped")
        db.session.rollback()
        raise

    if not is_full:
        return

    try:
        _cleanup(run_start)
        db.session.commit()
    except Exception:
        logger.exception("Cleanup failed -- rollback")
        db.session.rollback()
        raise

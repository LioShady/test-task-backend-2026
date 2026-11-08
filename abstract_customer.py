import logging
from typing import Dict, Iterable, List, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from amocrm import Customer, Lead
from models import (AbstractCustomer, AmoCompany, AmoContact, AmoCustomer,
                    AmoLead, db)

logger = logging.getLogger(__name__)


# --- подготовка строк для bulk-операций -------------------------------------

def customer_login(customer: Customer) -> Optional[str]:
    return customer.get_field('Логин')


def lead_login(lead: Lead) -> Optional[str]:
    return lead.get_field('Логин')


def customer_row(customer: Customer, abstract_customer_id: int) -> Dict:
    cfv = customer.custom_fields_values or {}
    return {
        'id': customer.id,
        'abstract_customer_id': abstract_customer_id,
        'login': customer_login(customer),
        'name': customer.name,
        'billing_type': customer.get_field('Вид оплаты'),
        'is_robot': bool(cfv.get('Робот')),
        'is_nds': bool(cfv.get('Оплата с НДС')),
        'system': customer.get_field('Система'),
        'host': customer.get_field('Хост'),
        'api_value': customer.as_dict() or {},
    }


def lead_row(lead: Lead, abstract_customer_id: int) -> Dict:
    return {
        'id': lead.id,
        'abstract_customer_id': abstract_customer_id,
        'login': lead_login(lead),
        'name': lead.name,
        'api_value': lead.as_dict() or {},
    }


def contact_row(contact: dict, abstract_customer_id: int) -> Dict:
    return {
        'id': contact.get('id'),
        'abstract_customer_id': abstract_customer_id,
        'name': contact.get('name'),
        'phone': contact.get('phone'),
        'api_value': contact or {},
    }


def company_row(company: dict, abstract_customer_id: int) -> Dict:
    return {
        'id': company.get('id'),
        'abstract_customer_id': abstract_customer_id,
        'name': company.get('name'),
        'inn': company.get('inn'),
        'api_value': company or {},
    }


def dedup_by_id(rows: Iterable[Dict]) -> List[Dict]:
    """Оставляет по одному dict на id (последний побеждает).

    Нужно перед bulk upsert
    """
    return list({r['id']: r for r in rows if r.get('id') is not None}.values())


# --- батчевые выборки --------------------------------------------------------

def fetch_amo_customers_by_ids(ids: Sequence[int]) -> Dict[int, AmoCustomer]:
    if not ids:
        return {}
    rows = db.session.scalars(
        select(AmoCustomer).where(AmoCustomer.id.in_(ids))
    ).all()
    return {r.id: r for r in rows}


def fetch_amo_leads_by_ids(ids: Sequence[int]) -> Dict[int, AmoLead]:
    if not ids:
        return {}
    rows = db.session.scalars(
        select(AmoLead).where(AmoLead.id.in_(ids))
    ).all()
    return {r.id: r for r in rows}


def fetch_abstract_customers_by_ids(ids: Sequence[int]) -> Dict[int, AbstractCustomer]:
    if not ids:
        return {}
    rows = db.session.scalars(
        select(AbstractCustomer).where(AbstractCustomer.id.in_(ids))
    ).all()
    return {r.id: r for r in rows}


def fetch_abstract_customer_ids_by_customer_login(logins: Sequence[str]) -> Dict[str, List[int]]:
    """Логин -> список abstract_customer_id, у которых есть покупатель с таким логином."""
    logins = [l for l in logins if l is not None]
    if not logins:
        return {}
    rows = db.session.execute(
        select(AmoCustomer.login, AmoCustomer.abstract_customer_id)
        .where(AmoCustomer.login.in_(logins))
    ).all()
    out: Dict[str, List[int]] = {}
    for login, ac_id in rows:
        out.setdefault(login, []).append(ac_id)
    return out


def fetch_abstract_customer_ids_by_lead_login(logins: Sequence[str]) -> Dict[str, List[int]]:
    """Логин -> список abstract_customer_id, у которых есть сделка с таким логином."""
    logins = [l for l in logins if l is not None]
    if not logins:
        return {}
    rows = db.session.execute(
        select(AmoLead.login, AmoLead.abstract_customer_id)
        .where(AmoLead.login.in_(logins))
    ).all()
    out: Dict[str, List[int]] = {}
    for login, ac_id in rows:
        out.setdefault(login, []).append(ac_id)
    return out


# --- bulk upsert -------------------------------------------------------------

def _upsert(model, rows: Iterable[Dict], conflict_cols: List[str]):
    rows = dedup_by_id(rows)
    if not rows:
        return
    stmt = pg_insert(model).values(rows)
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in model.__table__.columns
        if c.name not in conflict_cols and c.name not in ('created_at',)
    }
    if 'updated_at' in model.__table__.columns:
        update_cols['updated_at'] = db.func.now()
    stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols)
    db.session.execute(stmt)


def upsert_customers(rows: List[Dict]):
    _upsert(AmoCustomer, rows, conflict_cols=['id'])


def upsert_leads(rows: List[Dict]):
    _upsert(AmoLead, rows, conflict_cols=['id'])


def upsert_contacts(rows: List[Dict]):
    _upsert(AmoContact, rows, conflict_cols=['id'])


def upsert_companies(rows: List[Dict]):
    _upsert(AmoCompany, rows, conflict_cols=['id'])


def insert_abstract_customers(rows: List[Dict]) -> List[int]:
    """Вставка новых склеек с RETURNING id."""
    if not rows:
        return []
    stmt = (
        pg_insert(AbstractCustomer)
        .values(rows)
        .returning(AbstractCustomer.id)
    )
    return list(db.session.scalars(stmt).all())


def reassign_entities_to_customer(source_ids: Sequence[int], target_id: int):
    """Переносит сущности со склеек source_ids на target_id (слияние)."""
    source_ids = [s for s in source_ids if s != target_id]
    if not source_ids:
        return
    for model in (AmoCustomer, AmoLead, AmoContact, AmoCompany):
        db.session.execute(
            model.__table__.update()
            .where(model.abstract_customer_id.in_(source_ids))
            .values(abstract_customer_id=target_id)
        )


def delete_abstract_customers(ids: Sequence[int]):
    if not ids:
        return
    db.session.execute(
        AbstractCustomer.__table__.delete().where(AbstractCustomer.id.in_(ids))
    )

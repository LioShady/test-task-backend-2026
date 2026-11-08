from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import (BigInteger, Boolean, Column, DateTime, ForeignKey,
                        Index, MetaData, Text, false, func)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

naming_convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(column_0_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
db = SQLAlchemy(metadata=MetaData(naming_convention=naming_convention))


class AbstractCustomer(db.Model):
    """Клиент: склейка сущностей CRM, относящихся к одному и тому же логину.

    Связь «сделка - покупатель» - это общий abstract_customer_id у строк
    amo_lead и amo_customer. Удаление клиента каскадно удаляет его сущности.
    """
    __tablename__ = "abstract_customer"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    send_methods = Column(JSONB)  # [{"type": mail|sms|wtsp, "value": str, "desc": str}]
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, onupdate=func.now())

    customers = relationship("AmoCustomer", back_populates="abstract_customer",
                             cascade="all, delete", passive_deletes=True)
    leads = relationship("AmoLead", back_populates="abstract_customer",
                         cascade="all, delete", passive_deletes=True)
    contacts = relationship("AmoContact", back_populates="abstract_customer",
                            cascade="all, delete", passive_deletes=True)
    companies = relationship("AmoCompany", back_populates="abstract_customer",
                             cascade="all, delete", passive_deletes=True)


class _CrmEntityMixin:
    # Полный ответ API в прежнем формате.
    api_value = Column(JSONB, nullable=False)
    # Время старта последнего прогона, в котором сущность была в выгрузке.
    # Всё, что осталось со старой меткой после полного прохода, удаляется.
    synced_at = Column(DateTime, nullable=False, server_default=func.now())


def _abstract_customer_fk():
    return Column(BigInteger,
                  ForeignKey("abstract_customer.id", ondelete="CASCADE"),
                  nullable=False, index=True)


class AmoCustomer(_CrmEntityMixin, db.Model):
    __tablename__ = "amo_customer"
    id = Column(BigInteger, primary_key=True, autoincrement=False)  # id покупателя в CRM
    abstract_customer_id = _abstract_customer_fk()
    login = Column(Text, index=True)
    name = Column(Text, index=True)
    billing_type = Column(Text)
    is_robot = Column(Boolean, nullable=False, server_default=false())
    is_nds = Column(Boolean, nullable=False, server_default=false())
    system = Column(Text)
    host = Column(Text)

    abstract_customer = relationship("AbstractCustomer", back_populates="customers")

    __table_args__ = (
        Index("ix_amo_customer_login_lower", func.lower(login)),
        Index("ix_amo_customer_name_lower", func.lower(name)),
        Index("ix_amo_customer_synced_at", synced_at),
    )


class AmoLead(_CrmEntityMixin, db.Model):
    __tablename__ = "amo_lead"
    id = Column(BigInteger, primary_key=True, autoincrement=False)  # id сделки в CRM
    abstract_customer_id = _abstract_customer_fk()
    login = Column(Text, index=True)
    name = Column(Text, index=True)

    abstract_customer = relationship("AbstractCustomer", back_populates="leads")

    __table_args__ = (
        Index("ix_amo_lead_login_lower", func.lower(login)),
        Index("ix_amo_lead_name_lower", func.lower(name)),
        Index("ix_amo_lead_synced_at", synced_at),
    )


class AmoContact(_CrmEntityMixin, db.Model):
    __tablename__ = "amo_contact"
    id = Column(BigInteger, primary_key=True, autoincrement=False)  # id контакта в CRM
    abstract_customer_id = _abstract_customer_fk()
    name = Column(Text, index=True)
    phone = Column(Text, index=True)

    abstract_customer = relationship("AbstractCustomer", back_populates="contacts")

    __table_args__ = (
        Index("ix_amo_contact_name_lower", func.lower(name)),
        Index("ix_amo_contact_synced_at", synced_at),
    )


class AmoCompany(_CrmEntityMixin, db.Model):
    __tablename__ = "amo_company"
    id = Column(BigInteger, primary_key=True, autoincrement=False)  # id компании в CRM
    abstract_customer_id = _abstract_customer_fk()
    name = Column(Text, index=True)
    inn = Column(Text, index=True)

    abstract_customer = relationship("AbstractCustomer", back_populates="companies")

    __table_args__ = (
        Index("ix_amo_company_name_lower", func.lower(name)),
        Index("ix_amo_company_synced_at", synced_at),
    )

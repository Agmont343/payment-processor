from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB

from app.outbox.models import OutboxEvent
from app.payments.models import Payment, PaymentStatus


def test_payments_table_has_required_columns_and_constraints() -> None:
    columns = Payment.__table__.c

    assert columns.amount.type.precision == 18
    assert columns.amount.type.scale == 2
    assert columns.created_at.type.timezone is True
    assert columns.processed_at.type.timezone is True
    assert isinstance(columns.metadata_json.type, JSONB)
    assert columns.status.server_default is not None
    assert PaymentStatus.PENDING.value == "pending"

    constraint_names = {constraint.name for constraint in Payment.__table__.constraints}
    assert "ck_payments_amount_positive" in constraint_names
    assert "ck_payments_currency" in constraint_names
    assert "ck_payments_status" in constraint_names
    assert "uq_payments_idempotency_key" in constraint_names
    assert any(
        isinstance(item, CheckConstraint) for item in Payment.__table__.constraints
    )
    assert any(
        isinstance(item, UniqueConstraint) for item in Payment.__table__.constraints
    )


def test_outbox_has_unpublished_partial_index() -> None:
    columns = OutboxEvent.__table__.c

    assert columns.created_at.type.timezone is True
    assert columns.published_at.type.timezone is True
    assert isinstance(columns.payload.type, JSONB)
    index_names = {index.name for index in OutboxEvent.__table__.indexes}
    assert "ix_outbox_unpublished" in index_names
    unpublished = next(
        index
        for index in OutboxEvent.__table__.indexes
        if index.name == "ix_outbox_unpublished"
    )
    assert unpublished.dialect_options["postgresql"]["where"] is not None

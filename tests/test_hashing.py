from decimal import Decimal

from app.payments.hashing import canonical_request_hash


def _hash(**overrides: object) -> str:
    payload = {
        "amount": Decimal("10.5"),
        "currency": "RUB",
        "description": "Order",
        "metadata": {"b": 2, "a": 1},
        "webhook_url": "https://example.com/hook",
    }
    payload.update(overrides)
    return canonical_request_hash(
        amount=payload["amount"],  # type: ignore[arg-type]
        currency=payload["currency"],  # type: ignore[arg-type]
        description=payload["description"],  # type: ignore[arg-type]
        metadata=payload["metadata"],  # type: ignore[arg-type]
        webhook_url=payload["webhook_url"],  # type: ignore[arg-type]
    )


def test_hash_is_stable_for_equivalent_amount_and_metadata_order() -> None:
    left = _hash(
        amount=Decimal("10.5"),
        metadata={"b": 2, "a": 1, "nested": {"y": 2, "x": 1}},
    )
    right = _hash(
        amount=Decimal("10.50"),
        metadata={"nested": {"x": 1, "y": 2}, "a": 1, "b": 2},
    )
    assert left == right
    assert len(left) == 64


def test_hash_changes_when_body_changes() -> None:
    assert _hash(amount=Decimal("10.50")) != _hash(amount=Decimal("10.51"))
    assert _hash(description="Order") != _hash(description="Other")
    assert _hash(currency="RUB") != _hash(currency="USD")
    assert _hash(webhook_url="https://example.com/hook") != _hash(
        webhook_url="https://example.com/other"
    )
    assert _hash(metadata={"a": 1}) != _hash(metadata={"a": 2})

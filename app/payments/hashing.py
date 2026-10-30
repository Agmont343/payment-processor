import hashlib
import json
from decimal import Decimal
from typing import Any


def canonical_request_hash(
    *,
    amount: Decimal,
    currency: str,
    description: str,
    metadata: dict[str, Any],
    webhook_url: str,
) -> str:
    payload = {
        "amount": str(amount.quantize(Decimal("0.01"))),
        "currency": str(currency),
        "description": description,
        "metadata": metadata,
        "webhook_url": str(webhook_url),
    }
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

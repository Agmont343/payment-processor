from uuid import UUID


class PaymentNotFound(Exception):
    def __init__(self, payment_id: UUID) -> None:
        self.payment_id = payment_id
        super().__init__("Payment not found")


class IdempotencyConflict(Exception):
    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__("Idempotency-Key already used with a different request")

# Payment Processor

Асинхронный сервис обработки платежей: transactional outbox, потребитель на
RabbitMQ, webhook-уведомления с retry и DLQ.

**Стек:** Python 3.12, FastAPI, SQLAlchemy 2 (async), PostgreSQL 17, RabbitMQ 3.13,
FastStream, Alembic, Docker Compose.

## Возможности

- Создание платежей с идемпотентностью по `Idempotency-Key`.
- Transactional outbox: платёж и событие коммитятся атомарно, relay публикует
  события в RabbitMQ с publisher confirms.
- Consumer вызывает gateway-эмулятор и отправляет webhook; retry через
  TTL-очереди, после трёх неудач — DLQ.
- Защита webhook-URL от SSRF.

## Быстрый запуск

```bash
git clone <repository-url>
cd payment-processor
cp .env.docker.example .env.docker
docker compose up -d
```

## Сервисы и порты

| Сервис | Адрес |
|---|---|
| API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| RabbitMQ (AMQP) | localhost:5672 (guest/guest) |
| RabbitMQ Management UI | http://localhost:15672 (guest/guest) |
| PostgreSQL | localhost:55432 (postgres/postgres, БД `payments`) |

## API

Все эндпоинты, кроме `GET /health`, требуют заголовок `X-API-Key`.

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: your-key" \
  -H "Idempotency-Key: order-12345" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #12345",
    "metadata": {"order_id": "12345"},
    "webhook_url": "https://merchant.example.com/webhook"
  }'
```

- `202` — платёж создан; повтор с тем же `Idempotency-Key` и параметрами
  возвращает существующий платёж
- `409` — тот же `Idempotency-Key`, другие параметры
- `422` — невалидные данные
- `GET /api/v1/payments/{id}` — платёж; `404`, если не найден
- `GET /health` — healthcheck без авторизации

## Локальный запуск без Docker

```bash
uv sync
cp .env.example .env
```

В `.env` укажите адрес PostgreSQL с портом `55432` (`DB_URL` и `TEST_DB_URL`).

Миграции:

```bash
uv run alembic upgrade head
```

API и worker запускаются в двух отдельных терминалах.

Терминал 1:

```bash
uv run uvicorn app.main:app --reload --port 8000
```

Терминал 2:

```bash
uv run python -m app.worker
```

## Тестирование и линтинг

Для полного набора тестов нужны PostgreSQL и RabbitMQ (`docker compose up -d`).

```bash
uv run pytest
```

Тесты без RabbitMQ:

```bash
uv run pytest --ignore=tests/test_retry_ttl.py \
              --ignore=tests/test_pending_after_dlq_integration.py
```

Линтинг:

```bash
uv run ruff check .
uv run ruff format .
```

Интеграционные тесты работают только с БД `payments_test` и vhost `test`
на localhost — guard'ы в `tests/conftest.py` не дают им тронуть dev-окружение.

## Архитектура

- Идемпотентность держится на unique constraint `idempotency_key`: конкурентные
  запросы с одним ключом создают ровно один платёж, разные параметры — 409.
- Платёж и outbox-событие коммитятся в одной транзакции; relay выбирает события
  через `FOR UPDATE SKIP LOCKED` и публикует с publisher confirms.
- Доставка at-least-once: дубликат возможен при сбое между confirm брокера и
  записью `published_at` — дедупликация получателя по `event_id`.
- Consumer: чтение платежа → gateway → запись терминального статуса под
  `FOR UPDATE`. Три попытки обработки (TTL 1s и 2s), затем DLQ.
- Технический сбой до записи статуса оставляет платёж в `pending` — ожидаемое
  поведение; разбор по логам consumer и заголовкам `x-death` в DLQ.
- Webhook: 2xx — успех; 429, 5xx, таймауты — retry; прочие 4xx и 3xx — сразу в
  DLQ. URL-фильтр блокирует приватные адреса, но не защищает от DNS rebinding.
- Gateway-эмулятор детерминирован от `payment_id` (~10% бизнес-отказов) и не
  идемпотентен при crash — реальному провайдеру нужна своя идемпотентность.

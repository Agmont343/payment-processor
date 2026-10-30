# Payment Processor

Асинхронный сервис для обработки платежей с transactional outbox pattern и webhook-уведомлениями.

**Стек:** Python 3.12+, FastAPI, PostgreSQL 17, RabbitMQ 3.13, SQLAlchemy 2.0 (async), FastStream, Alembic

## Быстрый старт

```bash
# Клонирование и настройка
git clone <repository-url>
cd payment-processor
cp .env.example .env
# Отредактировать .env: установить API_KEY

# Запуск через Docker Compose
docker-compose up -d

# Проверка
curl http://localhost:8000/health  # {"status":"ok"}
```

**Сервисы:**
- API: http://localhost:8000
- RabbitMQ UI: http://localhost:15672 (guest/guest)
- PostgreSQL: localhost:5432

## API

Все эндпоинты требуют `X-API-Key` (кроме `/health`).

### POST /api/v1/payments

Создание платежа с идемпотентностью.

**Headers:** `X-API-Key`, `Idempotency-Key` (1-128 символов)

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

**Response 202:** платёж создан или возвращён существующий (при совпадении `Idempotency-Key` и параметров)  
**Response 409:** тот же `Idempotency-Key`, но другие параметры  
**Response 422:** невалидные данные

### GET /api/v1/payments/{id}

Получение платежа по ID. **Response 404:** платёж не найден.

### GET /health

Проверка работоспособности. **Не требует `X-API-Key`** (исключение для Docker healthcheck).

## Архитектура и гарантии

### Статусы платежей

- `pending` — обрабатывается
- `succeeded` — успешно обработан
- `failed` — отклонён шлюзом (бизнес-отказ)

### Идемпотентность API

- Unique constraint на `idempotency_key` (source of truth)
- Canonical hash параметров запроса
- Тот же ключ + те же параметры → существующий платёж (202)
- Тот же ключ + другие параметры → 409 Conflict

**Гарантия:** конкурентные запросы с одним ключом создают ровно один платёж.

### Transactional Outbox

Платёж и событие сохраняются **в одной транзакции PostgreSQL**. Фоновый relay публикует события в RabbitMQ с **publisher confirms**.

**⚠️ Ограничение:** Outbox не гарантирует **exactly-once** доставку. Если брокер подтвердил публикацию, но процесс упал до записи `published_at`, событие будет опубликовано повторно. Используйте `event_id` (= `outbox.id`) для дедупликации.

### Gateway Emulator

- Детерминированный: тот же `payment_id` → тот же исход (90% success / 10% failed)
- Задержка 2-5 секунд (детерминирована от `payment_id`)
- **⚠️ Не идемпотентен:** повторный вызов при незаписанном статусе возможен при crash. В production требуется идемпотентность на стороне провайдера.

### Webhook-уведомления

**Формат:**
```json
{
  "payment_id": "uuid",
  "status": "succeeded",
  "amount": "100.50",
  "currency": "RUB",
  "processed_at": "2026-10-30T15:30:03Z",
  "event_id": "uuid"
}
```

**`event_id`** — стабильный идентификатор для дедупликации.

**Классификация ответов:**
- 2xx → успех (ACK)
- 429, 5xx, timeout, сетевые ошибки → retry
- Остальные 4xx, 3xx → DLQ без retry

**⚠️ Гарантия:** Webhook доставляются **at-least-once**. Используйте `event_id` для дедупликации.

### Retry-механизм

**Три попытки обработки всего:**
1. Первая — из `payments.new`
2. Неудача → `payments.retry.1` (TTL **1s**) → обратно в `payments.new`
3. Неудача → `payments.retry.2` (TTL **2s**) → обратно в `payments.new`
4. Третья неудача → **DLQ**

**⚠️ Важно:** Это три попытки **обработки** (первая + две повторных), задержки 1s и 2s.

### Dead Letter Queue (DLQ)

Сообщение в DLQ при:
- Исчерпаны три попытки
- Постоянный сбой webhook (4xx кроме 429)
- Яд-сообщения (нет платежа в БД)

**⚠️ Платёж может остаться `pending` после DLQ:**

Если три попытки упали из-за технической ошибки **до** сохранения статуса (например, сбой БД при UPDATE), платёж останется `pending`, `processed_at = NULL`. Это нормально для технических сбоев.

**Действия:**
1. Проверить логи consumer (`payment_id` и причина)
2. RabbitMQ UI → Queues → `payments.new.dlq` (посмотреть `x-death` headers)
3. Исправить причину и вручную переместить из DLQ или обработать платёж

**Мониторинг:** `SELECT id FROM payments WHERE status='pending' AND created_at < NOW() - INTERVAL '10 minutes'`

### Webhook URL фильтрация

**Разрешены:** публичные HTTP/HTTPS адреса  
**Запрещены:** loopback (127.0.0.1), private networks (10.0.0.0/8, 192.168.0.0/16 и т.д.)

**⚠️ Ограничение:** Упрощённый фильтр, недостаточный для полной защиты от SSRF. Не защищает от **DNS rebinding** (DNS может измениться между валидацией и запросом). Для production: whitelist доменов, network policy, отдельная сеть для webhook.

**Для тестов:** `WEBHOOK_ALLOW_PRIVATE=true` в `.env`

## Разработка

### Установка

```bash
# Установить uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Установить зависимости
uv sync
```

### Локальный запуск без Docker

```bash
# Терминал 1: Инфраструктура
docker-compose up -d postgres rabbitmq

# Терминал 2: Миграции и API
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Терминал 3: Consumer
uv run python -m app.worker
```

**⚠️ Важно:** При локальном запуске используйте `.env` с адресами `localhost:5432` и `localhost:5672` (не `postgres:5432`/`rabbitmq:5672`). Контейнеры в Docker Compose используют `.env.docker` с именами сервисов.

### Тесты

```bash
# Unit-тесты (интеграционные модули исключены; DB-тесты скипаются без БД)
uv run pytest --ignore=tests/test_retry_ttl.py --ignore=tests/test_pending_after_dlq_integration.py

# Все тесты (требуют PostgreSQL и RabbitMQ; см. предупреждение ниже)
uv run pytest
```

> **⚠️ Windows и локальный PostgreSQL:** если на хостовой машине уже работает
> локальный PostgreSQL и порт 5432 занят, тесты, запущенные с хоста, могут
> подключиться к локальному серверу вместо контейнера — и выполняться не
> против той базы данных. Guard'ы в `tests/conftest.py` не дадут очищать
> чужую базу (тесты работают только с `payments_test` и выделенным vhost
> `test`), но надёжнее всего запускать тесты внутри сети Compose, не завися
> от портов хоста:
>
> ```bash
> docker compose up -d
> docker run --rm --network payment-processor_default \
>   -v "$(pwd):/w:ro" -w /w -e UV_PROJECT_ENVIRONMENT=/tmp/.venv \
>   -e DB_URL=postgresql+asyncpg://postgres:postgres@postgres:5432/payments \
>   -e TEST_DB_URL=postgresql+asyncpg://postgres:postgres@postgres:5432/payments_test \
>   -e RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/ \
>   -e TEST_RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/test \
>   ghcr.io/astral-sh/uv:python3.12-bookworm-slim \
>   sh -c "uv sync --frozen --dev && uv run --frozen pytest"
> ```
>
> Имя сети — `<папка-проекта>_default`, проверьте через `docker network ls`.
> В Git Bash на Windows добавьте `MSYS_NO_PATHCONV=1` перед `docker run`.
> Тесты безопасны для окружения по построению: они не трогают root-vhost и
> dev-БД `payments`.

### Линтинг

```bash
uv run ruff check .
uv run ruff format .
```

## Мониторинг

### Логи

```bash
docker-compose logs -f api
docker-compose logs -f consumer
```

### RabbitMQ UI

http://localhost:15672 (guest/guest)

**Важные очереди:**
- `payments.new` — основная
- `payments.retry.1`, `payments.retry.2` — TTL
- `payments.new.dlq` — DLQ (требует внимания)

### PostgreSQL

```bash
docker exec -it payment-processor-postgres-1 psql -U postgres -d payments

# Проверка статусов
SELECT status, COUNT(*) FROM payments GROUP BY status;

# Pending платежи
SELECT id, created_at FROM payments WHERE status='pending' ORDER BY created_at DESC;
```

## Production Checklist

- [ ] Сильный `API_KEY` (32+ случайных символа)
- [ ] Whitelist доменов для webhook URL
- [ ] TLS/HTTPS для API
- [ ] Мониторинг DLQ (алерты при messages > 0)
- [ ] Мониторинг pending платежей старше 10 минут
- [ ] Резервное копирование PostgreSQL
- [ ] Высокая доступность RabbitMQ
- [ ] Централизованное логирование
- [ ] Миграции через init-контейнер (не из N реплик API)

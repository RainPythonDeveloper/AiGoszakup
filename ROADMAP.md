# Roadmap: Пошаговый план реализации

## Этап 1: Фундамент (День 1)

### 1.1 Структура проекта
- [ ] Инициализировать Git-репозиторий
- [ ] Создать структуру директорий:
```
AiGoszakup/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── ARCHITECTURE.md
├── ROADMAP.md
├── src/
│   ├── ingestion/          # Слой 1: загрузка данных из API
│   │   ├── api_client.py   # REST/GraphQL клиент к OWS v3
│   │   ├── journal_sync.py # Инкрементальная синхронизация
│   │   └── rate_limiter.py # Rate limiting
│   ├── etl/                # Слой 2: ETL pipeline
│   │   ├── cleaner.py      # Очистка данных
│   │   ├── normalizer.py   # Нормализация
│   │   ├── enricher.py     # Обогащение
│   │   └── quality_gate.py # Контроль качества
│   ├── db/                 # Слой 3: БД
│   │   ├── schema.sql      # DDL всех таблиц
│   │   ├── views.sql       # Materialized Views
│   │   ├── indexes.sql     # Индексы
│   │   └── seed_refs.py    # Загрузка справочников
│   ├── analytics/          # Слой 4: Аналитика
│   │   ├── statistical.py  # IQR, MAD, Z-score
│   │   ├── ml_models.py    # Isolation Forest, LOF
│   │   └── fair_price.py   # Fair Price Calculator
│   ├── agent/              # Слой 5: AI Agent
│   │   ├── nlu.py          # Intent classification + NER
│   │   ├── planner.py      # Query planning
│   │   ├── executor.py     # SQL/Python sandbox
│   │   ├── composer.py     # Response formatting
│   │   └── tools.py        # Tool definitions для LLM
│   ├── api/                # Слой 6: REST API
│   │   ├── main.py         # FastAPI app
│   │   ├── routes.py       # Эндпоинты
│   │   └── schemas.py      # Pydantic models
│   └── config.py           # Конфигурация
├── dags/                   # Airflow DAGs
│   └── etl_pipeline.py
├── tests/
├── web/                    # Frontend (Chat UI)
└── notebooks/              # Jupyter для исследований
```

### 1.2 Инфраструктура
- [ ] Написать `docker-compose.yml` (PostgreSQL, Airflow, FastAPI)
- [ ] Настроить `.env` (API-токен, DB credentials)
- [ ] Поднять PostgreSQL в Docker
- [ ] Проверить подключение

### 1.3 Схема БД
- [ ] Написать `schema.sql` — все таблицы из ARCHITECTURE.md
- [ ] Написать `indexes.sql` — индексы по основным FK и полям фильтрации
- [ ] Применить миграции к PostgreSQL
- [ ] Проверить схему через `\dt` и `\d+ table_name`

**Deliverable:** Работающий PostgreSQL с пустой схемой в Docker.

---

## Этап 2: Подключение к API (День 1-2)

### 2.1 Изучение API
- [ ] Получить API-токен на goszakup.gov.kz
- [ ] Вручную протестировать 3-5 эндпоинтов через curl/Postman
- [ ] Понять реальную структуру JSON-ответов (могут отличаться от документации)
- [ ] Определить лимиты API (requests per minute, размер страницы)

### 2.2 API-клиент
- [ ] Реализовать `api_client.py`:
  - Авторизация Bearer-токен
  - Пагинация через `search_after`
  - Обработка ошибок (401, 429, 500)
  - Rate limiting (token bucket)
  - Retry с exponential backoff
  - Логирование всех запросов
- [ ] Написать тесты для API-клиента (mock-ответы)

### 2.3 Первая выгрузка
- [ ] Выгрузить справочники (КАТО, МКЕЙ, статусы, способы закупки) — они маленькие
- [ ] Загрузить справочники в `ref_*` таблицы
- [ ] Выгрузить 1 страницу объявлений (`/v3/trd-buy`) — проверить маппинг полей
- [ ] Выгрузить 1 страницу лотов (`/v3/lots`) — проверить маппинг

**Deliverable:** Работающий API-клиент + справочники загружены в БД.

---

## Этап 3: Data Ingestion — полная загрузка (День 2-3)

### 3.1 Bulk Loader
- [ ] Реализовать полную выгрузку для каждой сущности:
  - `subjects` (по списку БИНов)
  - `trd-buy` (объявления по БИНам за 2024-2026)
  - `lots` (лоты по БИНам)
  - `contracts` + `contract/subject` (договоры + предметы)
  - `plans` (годовые планы)
  - `trd-app` (заявки)
  - `payments` (платежи)
  - `contract-act` (акты)
- [ ] Сохранять raw JSON в таблицу `raw_data` (для аудита)
- [ ] Отслеживать прогресс загрузки (логирование количества загруженных записей)

### 3.2 Journal Sync
- [ ] Реализовать `journal_sync.py`:
  - Запрос `/v3/journal?date_from=...&date_to=...`
  - Парсинг действий (U = update, D = delete)
  - Точечная перезагрузка изменённых объектов
  - Запись в `sync_journal` таблицу
- [ ] Тест: запустить Journal Sync, проверить что подхватывает свежие изменения

**Deliverable:** Все данные за 2024-2026 загружены в raw_data. Journal Sync работает.

---

## Этап 4: ETL Pipeline (День 3-4)

### 4.1 Cleaning
- [ ] Реализовать `cleaner.py`:
  - Очистка названий лотов от HTML, спецсимволов, лишних пробелов
  - Стандартизация единиц измерения (маппинг на ref_units)
  - Обработка NULL и дубликатов
- [ ] Тест на реальных данных: посмотреть 50 "грязных" названий до/после

### 4.2 Normalization
- [ ] Реализовать `normalizer.py`:
  - БИН/ИИН -> 12 символов с ведущими нулями
  - Даты -> единый формат (datetime)
  - Суммы -> Decimal
  - КАТО коды -> маппинг на регионы
  - ЕНС ТРУ -> иерархия (parent_code)
- [ ] Тест: проверить что все FK резолвятся

### 4.3 Enrichment
- [ ] Реализовать `enricher.py`:
  - Расчёт `price_per_unit = amount / count`
  - Категоризация по верхнему уровню ЕНС ТРУ
  - Связывание лотов -> договоры -> планы
- [ ] ИПЦ: пока захардкодить таблицу индексов (позже автоматизировать)

### 4.4 Quality Gate
- [ ] Реализовать `quality_gate.py`:
  - Валидация: суммы > 0, даты в диапазоне, FK exist
  - Логирование проблем в `data_quality_log`
  - Отчёт: % отбракованных записей по каждой таблице
- [ ] Порог: если > 10% записей отбраковано — алерт

### 4.5 Загрузка в operational tables
- [ ] Трансформированные данные из raw_data -> operational tables
  (subjects, announcements, lots, contracts, contract_subjects, plans, ...)
- [ ] Проверить: `SELECT COUNT(*) FROM каждая_таблица`

**Deliverable:** Чистые данные в operational tables. Quality report.

---

## Этап 5: Аналитический слой (День 4-5)

### 5.1 Materialized Views
- [ ] Создать `mv_price_statistics` (средневзвешенные цены по ENSTRU + регион + квартал)
- [ ] Создать `mv_volume_trends` (объёмы по годам для выявления завышений)
- [ ] Создать `mv_supplier_stats` (статистика поставщиков)
- [ ] Проверить данные: ручная выборка 5-10 записей, сверка с порталом

### 5.2 Statistical Module
- [ ] Реализовать `statistical.py`:
  - `detect_price_outliers_iqr(enstru_code, kato, period)` -> list
  - `calculate_mad(prices)` -> float
  - `calculate_zscore(price, enstru_code)` -> float
  - `percentile_rank(price, enstru_code)` -> float
- [ ] Тест: запустить на реальных данных, проверить что находит аномалии

### 5.3 ML Models
- [ ] Реализовать `ml_models.py`:
  - `train_isolation_forest(enstru_code)` -> model
  - `train_lof(enstru_code, kato)` -> model
  - `predict_anomaly(model, features)` -> score
- [ ] Обучить модели на загруженных данных
- [ ] Сравнить результаты IF vs IQR — пересечение аномалий

### 5.4 Fair Price Calculator
- [ ] Реализовать `fair_price.py`:
  - `calculate_fair_price(enstru, kato, date)` -> FairPriceResult
  - `calculate_regional_coeff(enstru, kato)` -> float
  - `adjust_for_inflation(price, from_date, to_date)` -> float
  - `assess_fairness(actual_price, fair_price)` -> verdict
- [ ] Тест на 10 реальных лотах: ручная проверка вердиктов

**Deliverable:** Работающие аналитические функции. Fair Price формула проверена.

---

## Этап 6: AI Agent (День 5-6)

### 6.1 Tools (инструменты для LLM)
- [ ] Определить tools в `tools.py`:
  - `execute_sql(query: str)` -> DataFrame
  - `calculate_statistics(data, method: str)` -> dict
  - `detect_anomalies(enstru_code, method: str)` -> list
  - `calculate_fair_price(enstru, kato, date)` -> dict
- [ ] Read-only SQL connection (защита от мутаций)
- [ ] Python sandbox (whitelist модулей, лимиты CPU/RAM)

### 6.2 System Prompt
- [ ] Написать system prompt для LLM:
  - Схема БД (DDL + описание таблиц + примеры данных)
  - Список доступных tools с описанием
  - Формат ответа (6 секций из ТЗ)
  - Правила: отвечать ТОЛЬКО на основе данных, без домыслов
  - Правила Explainability (ID, выборка, метод, уверенность)
  - Примеры вопросов и эталонных ответов (few-shot)

### 6.3 ReAct Agent
- [ ] Реализовать агент (LangChain или чистый API):
  - Цикл: Think -> Act (tool call) -> Observe -> Think -> ... -> Answer
  - Ограничение: max 10 tool calls на один вопрос
  - Логирование каждого шага (для отладки и аудита)
- [ ] Подключить LLM (Claude API / GPT-4 API)

### 6.4 Тестирование агента
- [ ] Протестировать обязательные типы вопросов из ТЗ:
  - Аномалии: "Найди закупки с отклонением цены > 30% от средневзвешенной по ENSTРУ X"
  - Справедливость: "Оцени адекватность цены лота №..."
  - Объёмы: "Выяви нетипичное завышение количества ТРУ по сравнению с предыдущими годами"
- [ ] Проверить формат ответов (все 6 секций)
- [ ] Проверить Explainability (ID, N, метод, уверенность)
- [ ] Замерить точность извлечения фактов (цифры, даты, БИН) — цель >= 85%

**Deliverable:** Работающий AI-агент, отвечающий на вопросы из ТЗ.

---

## Этап 7: Interface + Сборка (День 6-7)

### 7.1 REST API
- [ ] Реализовать FastAPI:
  - `POST /api/ask` — отправить вопрос агенту
  - `GET /api/ask/{id}` — получить результат (async)
  - `GET /api/health` — healthcheck
  - `GET /api/stats` — статистика по данным (количество записей, дата последнего обновления)
- [ ] WebSocket для стриминга ответов

### 7.2 Chat UI
- [ ] Простой веб-интерфейс (Streamlit или React):
  - Поле ввода вопроса
  - Отображение ответа (markdown)
  - Индикатор загрузки
  - История вопросов
  - Поддержка KZ/RU

### 7.3 Airflow DAG
- [ ] Написать DAG `etl_pipeline.py`:
  - Task 1: `journal_sync` (инкрементальная загрузка)
  - Task 2: `run_etl` (cleaning -> normalization -> enrichment -> quality gate)
  - Task 3: `refresh_views` (REFRESH MATERIALIZED VIEW)
  - Task 4: `retrain_models` (IF, LOF)
  - Schedule: каждые 12 часов

### 7.4 Docker Compose
- [ ] Финализировать `docker-compose.yml`:
  - `postgres` (с volume для persistence)
  - `airflow` (scheduler + worker + webserver)
  - `agent` (FastAPI + AI Agent)
  - `web` (Chat UI)
- [ ] Протестировать полный запуск: `docker-compose up`
- [ ] Написать `README.md` с инструкцией по запуску

### 7.5 Финальное тестирование
- [ ] End-to-end тест: вопрос через Chat UI -> ответ с цифрами и ссылками
- [ ] Проверить все 3 обязательных типа вопросов
- [ ] Проверить инкрементальное обновление (добавить новые данные, подождать 12h)
- [ ] Подготовить примеры ответов AI-агента для сдачи

**Deliverable:** Полностью работающая система в Docker. Готова к демо.

---

## Порядок запуска (Quick Start)

```bash
# 1. Клонировать и настроить
git clone <repo>
cp .env.example .env
# Заполнить .env: API_TOKEN, DB_PASSWORD

# 2. Поднять инфраструктуру
docker-compose up -d postgres airflow

# 3. Применить схему БД
docker-compose exec postgres psql -U user -d goszakup -f /schema.sql

# 4. Первичная загрузка данных
docker-compose run agent python -m src.ingestion.bulk_load

# 5. Запустить ETL
docker-compose run agent python -m src.etl.pipeline

# 6. Запустить агента + UI
docker-compose up -d agent web

# 7. Открыть в браузере
open http://localhost:8501
```

---

## С чего начинаем прямо сейчас

**Шаг 1:** Создать структуру проекта + docker-compose.yml + schema.sql

Это фундамент, без которого ничего не работает. После этого сразу переходим к API-клиенту и первой выгрузке данных.

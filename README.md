# 🏛️ AI Госзакупки РК

AI-агент для анализа государственных закупок Республики Казахстан.  
Система загружает данные из [goszakup.gov.kz](https://ows.goszakup.gov.kz) (API OWS v3), очищает, обогащает их и предоставляет интеллектуальный чат-интерфейс с поддержкой казахского и русского языков.

---

## 📋 Оглавление

- [Архитектура](#архитектура)
- [Стек технологий](#стек-технологий)
- [Предварительные требования](#предварительные-требования)
- [Быстрый старт (Docker)](#быстрый-старт-docker)
- [Локальный запуск (без Docker)](#локальный-запуск-без-docker)
- [CLI-команды](#cli-команды)
- [Переменные окружения](#переменные-окружения)
- [Структура проекта](#структура-проекта)
- [Airflow DAG](#airflow-dag)
- [Kubernetes](#kubernetes)
- [Документация](#документация)

---

## Архитектура

```
goszakup.gov.kz API OWS v3 (REST + GraphQL)
                    │
          ┌─────────▼──────────┐
          │  1. Data Ingestion │  ← Airflow / run.py
          └─────────┬──────────┘
          ┌─────────▼──────────┐
          │  2. ETL Pipeline   │
          └─────────┬──────────┘
          ┌─────────▼──────────┐
          │  3. PostgreSQL     │
          └─────────┬──────────┘
          ┌─────────▼──────────┐
          │  4. Analytics      │  ← IQR, Isolation Forest, Fair Price
          └─────────┬──────────┘
          ┌─────────▼──────────┐
          │  5. AI Agent (LLM) │  ← ReAct-агент
          └─────────┬──────────┘
          ┌─────────▼──────────┐
          │  6. Chat Interface │  ← FastAPI + WebSocket
          └────────────────────┘
```

Подробнее: [ARCHITECTURE.md](ARCHITECTURE.md)

---

## Стек технологий

| Компонент       | Технология                                |
|-----------------|-------------------------------------------|
| Язык            | Python 3.11+                              |
| База данных     | PostgreSQL 16                             |
| API-сервер      | FastAPI + Uvicorn                         |
| HTTP-клиент     | httpx (async)                             |
| ML              | scikit-learn (Isolation Forest, LOF)      |
| LLM             | OpenAI-совместимый API (nitec-ai.kz)      |
| Оркестрация     | Apache Airflow                            |
| Контейнеризация | Docker, Docker Compose                    |
| Деплой          | Kubernetes (Helm-совместимые манифесты)   |

---

## Предварительные требования

- **Python** >= 3.11
- **Docker** и **Docker Compose** (для контейнерного запуска)
- **PostgreSQL 16** (при локальном запуске без Docker)
- **API-токен** goszakup.gov.kz ([получить на портале](https://ows.goszakup.gov.kz))
- **LLM API-ключ** (nitec-ai.kz или иной OpenAI-совместимый провайдер)

---

## Быстрый старт (Docker)

Самый простой способ запустить проект — через Docker Compose. Он поднимет PostgreSQL и приложение автоматически.

### 1. Клонируйте репозиторий

```bash
git clone <repo-url>
cd AiGoszakup
```

### 2. Настройте переменные окружения

```bash
cp .env.example .env
```

Откройте `.env` и заполните обязательные поля:

```dotenv
GOSZAKUP_API_TOKEN=ваш_bearer_токен
LLM_API_KEY=ваш_llm_ключ
POSTGRES_PASSWORD=надёжный_пароль
```

### 3. Запустите

```bash
docker-compose up -d
```

Это запустит:
- **PostgreSQL 16** на порту `5433` (с автоматической инициализацией схемы БД)
- **Приложение** (FastAPI) на порту `8000`

### 4. Откройте в браузере

```
http://localhost:8000
```

### Остановка

```bash
docker-compose down
```

Для полного удаления данных (включая БД):

```bash
docker-compose down -v
```

---

## Локальный запуск (без Docker)

### 1. Создайте виртуальное окружение

```bash
python3.11 -m venv venv
source venv/bin/activate   # macOS / Linux
```

### 2. Установите зависимости

```bash
pip install -r requirements.txt
```

### 3. Настройте `.env`

```bash
cp .env.example .env
# Отредактируйте .env — укажите токены и параметры БД
```

### 4. Запустите PostgreSQL

Если PostgreSQL не запущен, `run.py` попытается поднять его через Docker автоматически:

```bash
docker-compose up -d postgres
```

### 5. Запустите проект

```bash
python3 run.py
```

Эта команда выполнит полный цикл:
1. ✅ Проверит подключение к PostgreSQL
2. 📚 Загрузит справочники (КАТО, МКЕЙ, способы закупки, статусы)
3. 📦 Загрузит данные из API (объявления, лоты, договоры, планы) — **10–30 минут**
4. 🔄 Запустит ETL-пайплайн (очистка, нормализация, обогащение)
5. 🚀 Запустит FastAPI-сервер на `http://localhost:8000`

---

## CLI-команды

`run.py` поддерживает несколько режимов запуска:

| Команда                  | Описание                                               |
|--------------------------|--------------------------------------------------------|
| `python3 run.py`         | Полный цикл: БД → загрузка → ETL → сервер             |
| `python3 run.py server`  | Только запуск сервера (данные уже загружены)            |
| `python3 run.py load`    | Только загрузка данных из API                          |
| `python3 run.py etl`     | Только ETL-пайплайн (очистка + обновление views)       |
| `python3 run.py status`  | Показать текущий статус данных в БД                    |

---

## Переменные окружения

Все переменные описаны в файле [.env.example](.env.example):

| Переменная              | Описание                                   | По умолчанию                         |
|-------------------------|--------------------------------------------|--------------------------------------|
| `GOSZAKUP_API_TOKEN`    | Bearer-токен API goszakup.gov.kz           | —                                    |
| `GOSZAKUP_API_BASE_URL` | Базовый URL API                            | `https://ows.goszakup.gov.kz`       |
| `POSTGRES_HOST`         | Хост PostgreSQL                            | `localhost`                          |
| `POSTGRES_PORT`         | Порт PostgreSQL                            | `5433`                               |
| `POSTGRES_DB`           | Имя базы данных                            | `goszakup`                           |
| `POSTGRES_USER`         | Пользователь БД                            | `goszakup`                           |
| `POSTGRES_PASSWORD`     | Пароль БД                                  | —                                    |
| `LLM_PROVIDER`          | Провайдер LLM                              | `openai_compatible`                  |
| `LLM_BASE_URL`          | URL LLM API                                | `https://nitec-ai.kz/api`           |
| `LLM_API_KEY`           | API-ключ для LLM                           | —                                    |
| `LLM_MODEL`             | Модель LLM                                 | `openai/gpt-oss-120b`               |
| `APP_PORT`              | Порт приложения                            | `8000`                               |
| `LOG_LEVEL`             | Уровень логирования                        | `INFO`                               |

---

## Структура проекта

```
AiGoszakup/
├── run.py                  # Единая точка запуска (CLI)
├── requirements.txt        # Python-зависимости
├── Dockerfile              # Multi-stage Docker-образ
├── docker-compose.yml      # Docker Compose (PostgreSQL + App)
├── .env.example            # Шаблон переменных окружения
│
├── src/                    # Исходный код
│   ├── config.py           # Конфигурация (чтение .env)
│   ├── ingestion/          # Слой загрузки данных из API
│   ├── etl/                # ETL-пайплайн (очистка, обогащение)
│   ├── db/                 # SQL-схемы, индексы, views
│   ├── analytics/          # Статистический и ML-анализ
│   ├── agent/              # AI-агент (ReAct, LLM)
│   ├── api/                # FastAPI-сервер + WebSocket
│   └── evaluation/         # Оценка качества ответов
│
├── dags/                   # Airflow DAG для инкрементальной синхронизации
│   └── goszakup_sync.py
│
├── k8s/                    # Kubernetes-манифесты
│   ├── namespace.yaml
│   ├── secret.yaml
│   ├── configmap.yaml
│   ├── postgres.yaml
│   └── app.yaml
│
├── docs/                   # Документация
│   ├── EXAMPLES.md         # Примеры запросов
│   └── RISKS.md            # Риски и ограничения
│
├── notebooks/              # Jupyter-ноутбуки для исследований
├── tests/                  # Тесты
├── web/                    # Веб-интерфейс (frontend)
│
├── ARCHITECTURE.md         # Подробная архитектура (6 слоёв)
└── ROADMAP.md              # Дорожная карта проекта
```

---

## Airflow DAG

Файл `dags/goszakup_sync.py` реализует автоматическую инкрементальную синхронизацию, которая запускается каждые 12 часов:

```
incremental_sync → run_etl → refresh_views
```

1. **incremental_sync** — читает журнал изменений API, дозагружает обновлённые записи
2. **run_etl** — очистка и обогащение данных
3. **refresh_views** — обновление 5 materialized views (ценовая статистика, тренды объёмов, статистика поставщиков, региональные коэффициенты, обзор данных)

Для использования Airflow скопируйте `dags/` в директорию DAGs вашего Airflow-инстанса.

---

## Kubernetes

Манифесты для деплоя в Kubernetes находятся в директории `k8s/`:

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/app.yaml
```

> ⚠️ Перед деплоем обновите `k8s/secret.yaml` — укажите свои токены и пароли (в base64).

---

## Документация

| Документ                              | Описание                                 |
|---------------------------------------|------------------------------------------|
| [ARCHITECTURE.md](ARCHITECTURE.md)    | Полная архитектура системы (6 слоёв)     |
| [ROADMAP.md](ROADMAP.md)             | Дорожная карта и план развития           |
| [docs/EXAMPLES.md](docs/EXAMPLES.md) | Примеры запросов к AI-агенту             |
| [docs/RISKS.md](docs/RISKS.md)       | Риски и ограничения                      |

---

## Лицензия

Проект разработан для внутреннего использования.

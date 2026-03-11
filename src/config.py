"""
Конфигурация проекта. Все настройки читаются из .env файла.
"""
import os
from pathlib import Path
from dataclasses import dataclass

from dotenv import load_dotenv

# Корень проекта
ROOT_DIR = Path(__file__).parent.parent
load_dotenv(ROOT_DIR / ".env")


@dataclass
class APIConfig:
    token: str = os.getenv("GOSZAKUP_API_TOKEN", "")
    base_url: str = os.getenv("GOSZAKUP_API_BASE_URL", "https://ows.goszakup.gov.kz")
    page_size: int = 50  # стандартный размер страницы OWS v3
    max_retries: int = 3
    retry_delay: float = 2.0  # секунд (base для exponential backoff)
    requests_per_second: float = 5.0  # rate limit


@dataclass
class DBConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "goszakup")
    user: str = os.getenv("POSTGRES_USER", "goszakup")
    password: str = os.getenv("POSTGRES_PASSWORD", "changeme_strong_password")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def async_dsn(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class LLMConfig:
    provider: str = os.getenv("LLM_PROVIDER", "anthropic")
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    model: str = os.getenv("LLM_MODEL", "claude-sonnet-4-5-20250929")
    max_tokens: int = 4096
    temperature: float = 0.0  # детерминированные ответы для воспроизводимости


@dataclass
class AppConfig:
    port: int = int(os.getenv("APP_PORT", "8000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


# Список БИНов организаций для выгрузки (заполнить из ТЗ)
TARGET_BINS: list[str] = [
    # TODO: заполнить из перечня организаций в ТЗ
]

# Глубина данных
DATA_YEARS = [2024, 2025, 2026]

# Singleton-конфигурации
api_config = APIConfig()
db_config = DBConfig()
llm_config = LLMConfig()
app_config = AppConfig()

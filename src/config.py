from functools import lru_cache
from typing import Optional

from pydantic import Field, PostgresDsn, RedisDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
  model_config = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
    case_sensitive=False,
    extra="ignore"
  )

# Application
  app_env: str = Field(default="development", alias="APP_ENV")
  log_level: str = Field(default="INFO", alias="LOG_LEVEL")
  debug: bool = Field(default=False)

# Merchant padrão (seeded no banco)
  default_merchant_id: str = Field(default="6758", alias="DEFAULT_MERCHANT_ID")

# Database
  database_url: PostgresDsn = Field(alias="DATABASE_URL")
  database_pool_size: int = Field(default=20, alias="DATABASE_POOL_SIZE")
  database_max_overflow: int = Field(default=10, alias="DATABASE_MAX_OVERFLOW")
  database_echo: bool = Field(default=False)  # Log SQL queries (dev only)
  
# Redis
  redis_url: RedisDsn = Field(alias="REDIS_URL")
  redis_socket_timeout: int = 5
  redis_socket_connect_timeout: int = 5
  redis_retry_on_timeout: bool = True
  

# Security
  webhook_secret_token: str = Field(alias="WEBHOOK_SECRET_TOKEN")
  api_key_header: str = Field(default="X-API-Key", alias="API_KEY_HEADER")

# Timing-safe comparison para tokens
  webhook_token_max_age_seconds: int = 300  # 5 minutos tolerância

# Worker
  worker_enabled: bool = Field(default=True, alias="WORKER_ENABLED")
  worker_poll_interval: int = Field(default=5, alias="WORKER_POLL_INTERVAL")
  worker_batch_size: int = Field(default=10, alias="WORKER_BATCH_SIZE")
  worker_max_retries: int = 3
  worker_retry_delay: int = 5  # segundos

# Cardapioweb APIs (enriquecimento)
  cardapioweb_public_base_url: str = "https://app.cardapioweb.com/api/v1"
  cardapioweb_dashboard_base_url: str = "https://app.cardapioweb.com/dashboard/api"
  cardapioweb_api_timeout: int = 10  # segundos
  

# Geo/Features
  earth_radius_km: float = 6371.0

  @field_validator("log_level")
  @classmethod
  def validate_log_level(cls, v: str) -> str:
    allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    v_upper = v.upper()
    if v_upper not in allowed:
        raise ValueError(f"LOG_LEVEL deve ser um de: {allowed}")
    return v_upper
  
  @field_validator("app_env")
  @classmethod
  def validate_app_env(cls, v: str) -> str:
    return v.lower()
  
  @property
  def is_production(self) -> bool:
    return self.app_env == "production"
  
  @property
  def is_development(self) -> bool:
    return self.app_env == "development"
  
  @property
  def database_url_async(self) -> str:
    """
    Retorna URL do banco garantindo driver asyncpg.
    Converte postgresql:// para postgresql+asyncpg:// se necessário.
    """
    url = str(self.database_url)
    if "postgresql://" in url and "asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


@lru_cache()
def get_settings() -> Settings:
    """
    Retorna instância cacheada de Settings.
    Cache evita re-leitura do .env a cada chamada.
    """
    return Settings()


# Export singleton para import conveniente
settings = get_settings()
# ============================================
# CONFIGURAÇÃO DA APLICAÇÃO
# ============================================

from functools import lru_cache
from typing import Optional

from pydantic import Field, PostgresDsn, RedisDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuração centralizada da aplicação.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # --------------------------------------------
    # Application
    # --------------------------------------------
    app_env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    debug: bool = Field(default=False)
    default_merchant_id: str = Field(default="6758", alias="DEFAULT_MERCHANT_ID")
    
    # --------------------------------------------
    # Database
    # --------------------------------------------
    database_url: PostgresDsn = Field(alias="DATABASE_URL")
    database_pool_size: int = Field(default=20, alias="DATABASE_POOL_SIZE")
    database_max_overflow: int = Field(default=10, alias="DATABASE_MAX_OVERFLOW")
    database_echo: bool = Field(default=False)
    
    # --------------------------------------------
    # Redis
    # --------------------------------------------
    redis_url: RedisDsn = Field(alias="REDIS_URL")
    redis_socket_timeout: int = 5
    redis_socket_connect_timeout: int = 5
    redis_retry_on_timeout: bool = True
    
    # --------------------------------------------
    # Security
    # --------------------------------------------
    webhook_secret_token: str = Field(alias="WEBHOOK_SECRET_TOKEN")
    api_key_header: str = Field(default="X-API-Key", alias="API_KEY_HEADER")
    webhook_token_max_age_seconds: int = 300
    
    # --------------------------------------------
    # Worker
    # --------------------------------------------
    worker_enabled: bool = Field(default=True, alias="WORKER_ENABLED")
    worker_poll_interval: int = Field(default=5, alias="WORKER_POLL_INTERVAL")
    worker_batch_size: int = Field(default=10, alias="WORKER_BATCH_SIZE")
    worker_max_retries: int = 3
    worker_retry_delay: int = 5
    
    # --------------------------------------------
    # Cardapioweb APIs (Etapa 4) - COM VALIDAÇÃO
    # --------------------------------------------
    cardapioweb_public_base_url: str = Field(
        alias="CARDAPIOWEB_PUBLIC_BASE_URL"
    )
    cardapioweb_dashboard_base_url: str = Field(
        alias="CARDAPIOWEB_DASHBOARD_BASE_URL"
    )
    cardapioweb_public_api_key: str = Field(
        alias="CARDAPIOWEB_PUBLIC_API_KEY"
    )
    cardapioweb_dashboard_api_key: str = Field(
        alias="CARDAPIOWEB_DASHBOARD_API_KEY"
    )
    cardapioweb_api_timeout: int = Field(
        default=10,
        alias="CARDAPIOWEB_API_TIMEOUT"
    )
    
    # --------------------------------------------
    # Geo
    # --------------------------------------------
    earth_radius_km: float = 6371.0
    
    # --------------------------------------------
    # Validações
    # --------------------------------------------
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
    
    @field_validator("cardapioweb_public_base_url", "cardapioweb_dashboard_base_url")
    @classmethod
    def validate_urls(cls, v: str) -> str:
        """Remove trailing slash se presente."""
        return v.rstrip('/')
    
    @property
    def is_production(self) -> bool:
        return self.app_env == "production"
    
    @property
    def is_development(self) -> bool:
        return self.app_env == "development"
    
    @property
    def database_url_async(self) -> str:
        url = str(self.database_url)
        if "postgresql://" in url and "asyncpg" not in url:
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        if "asyncpg" not in url:
            raise ValueError("DATABASE_URL deve usar driver asyncpg")
        return url


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
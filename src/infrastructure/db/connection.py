from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from src.config import settings

# Engine singleton
_engine = None
_async_session_maker = None


def get_engine():
    """Retorna engine SQLAlchemy (singleton)."""
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.database_url_async,
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
            pool_pre_ping=True,  # Verifica conexão antes de usar
            pool_recycle=300,  # Recicla conexões após 5 min
            echo=settings.database_echo,
            poolclass=NullPool if settings.app_env == "testing" else None,
        )
    return _engine


def get_session_maker() -> async_sessionmaker[AsyncSession]:
    global _async_session_maker
    if _async_session_maker is None:
        _async_session_maker = async_sessionmaker(
            bind=get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _async_session_maker


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    session = get_session_maker()()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with get_db_session() as session:
        yield session


async def init_db():
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute("SELECT 1")
        assert result.scalar() == 1
    return engine


async def close_db():
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None

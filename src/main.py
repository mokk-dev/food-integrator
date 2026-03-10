import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import text

from src.config import settings
from src.infrastructure.cache.redis_client import redis_client
from src.infrastructure.db.connection import close_db, init_db
from src.api.routes import webhooks, admin
from src.core.logger import logger
from src.infrastructure.external.cardapioweb_auth import CardapiowebAuthManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ========== STARTUP ==========
    logger.info("startup.starting", version="15.0.0", env=settings.app_env)

    try:
        await init_db()
        logger.info("startup.database_connected")
    except Exception as e:
        logger.error("startup.database_failed", error=str(e))
        raise

    try:
        await redis_client.connect()
        logger.info("startup.redis_connected")
    except Exception as e:
        logger.error("startup.redis_failed", error=str(e))
        raise

    # Cria tabelas se não existirem (dev only - em prod usar migrations)
    # if settings.is_development:
    #     from infrastructure.db.connection import get_engine
    #     from sqlalchemy import MetaData
    #     async with get_engine().begin() as conn:
    #         # Não criamos tabelas aqui - SQL de initdb cuida disso
    #         pass

    logger.info("startup.ready")

    yield

    # ========== SHUTDOWN ==========
    logger.info("shutdown.starting")

    await close_db()
    await redis_client.disconnect()

    logger.info("shutdown.connections_closed")


app = FastAPI(
    title="Cardapioweb Integrator",
    description="Integração de webhooks Cardapioweb para analytics operacional",
    version="15.0.0",
    docs_url="/docs" if not settings.is_production else None,
    redoc_url="/redoc" if not settings.is_production else None,
    openapi_url="/openapi.json" if not settings.is_production else None,
    lifespan=lifespan,
)


# MIDDLEWARE


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))

    request.state.correlation_id = correlation_id

    response = await call_next(request)

    response.headers["X-Correlation-ID"] = correlation_id

    return response


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = (
        "max-age=31536000; includeSubDomains"
    )

    return response


@app.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "healthy",
        "version": "15.0.0",
        "environment": settings.app_env,
    }


@app.get("/ready", tags=["Health"])
async def readiness_check():
    checks = {}
    status_code = 200

    try:
        from src.infrastructure.db.connection import get_engine
        engine = get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {str(e)}"
        status_code = 503
    
    try:
        from src.infrastructure.cache.redis_client import redis_client
        if redis_client._client is None:
            await redis_client.connect()
        await redis_client.client.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {str(e)}"
        status_code = 503
    
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "ready" if status_code == 200 else "not_ready",
            "checks": checks
        }
    )


@app.get("/auth/status", tags=["Auth", "Health"])
async def auth_status_check(force_refresh: bool = False):
    """
    Verifica a saúde da autenticação com o Dashboard da Cardapioweb.
    Se force_refresh=True, ignora o cache e testa ativamente a negociação 
    de um novo token usando o Refresh Token atual.
    """
    try:
        auth_manager = CardapiowebAuthManager()
        
        # Tenta obter o token. Se force_refresh for passado, ele obriga 
        # o auth_manager a bater na API da Cardapioweb para validar o refresh_token.
        await auth_manager.get_valid_access_token(force_refresh=force_refresh)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "online",
                "message": "Tokens válidos e operacionais.",
                "action_required": None
            }
        )
        
    except Exception as e:
        logger.error("auth.healthcheck_failed", error=str(e))
        
        return JSONResponse(
            status_code=503,
            content={
                "status": "offline",
                "error": "Falha de Autenticação na Plataforma",
                "detail": str(e),
                "action_required": "É necessário injetar uma nova sessão humana válida (novo refresh_token)"
            }
        )


app.include_router(
    webhooks.router,
    prefix="/webhook",
    tags=["Webhooks"]
)

app.include_router(
    admin.router,
    prefix="/api/admin",
    tags=["Admin"]
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    detail = str(exc) if settings.is_development else "Internal server error"

    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_error",
            "detail": detail,
            "correlation_id": getattr(request.state, "correlation_id", None),
        },
    )

@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Handler personalizado para 404."""
    return JSONResponse(
        status_code=404,
        content={
            "error": "not_found",
            "detail": "Resource not found",
            "correlation_id": getattr(request.state, "correlation_id", None)
        }
    )
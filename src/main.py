import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import text

from src.config import settings
from src.infrastructure.cache.redis_client import redis_client
from src.infrastructure.db.connection import close_db, init_db

# TODO: Import das rotas (serão criadas na Etapa 3)
# from api.routes import webhooks, health, admin


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ========== STARTUP ==========
    print("🚀 Iniciando Cardapioweb Integrator v15")
    print(f"   Environment: {settings.app_env}")
    print(f"   Log Level: {settings.log_level}")

    try:
        await init_db()
        print("Database connected")
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

    try:
        await redis_client.connect()
        print("✅ Redis connected")
    except Exception as e:
        print(f"Redis connection failed: {e}")
        raise

    # Cria tabelas se não existirem (dev only - em prod usar migrations)
    # if settings.is_development:
    #     from infrastructure.db.connection import get_engine
    #     from sqlalchemy import MetaData
    #     async with get_engine().begin() as conn:
    #         # Não criamos tabelas aqui - SQL de initdb cuida disso
    #         pass

    print("Aplicação pronta para receber requisições")

    yield  # Aplicação rodando...

    # ========== SHUTDOWN ==========
    print("Encerrando aplicação...")

    await close_db()
    await redis_client.disconnect()

    print("✅ Conexões fechadas")


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
    
    # Check DB
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
    
    # Check Redis
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


# TODO: Registrar rotas na Etapa 3
# app.include_router(webhooks.router, prefix="/webhook", tags=["Webhooks"])
# app.include_router(admin.router, prefix="/admin", tags=["Admin"])


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
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"error": "not_found", "detail": "Resource not found"}
    )
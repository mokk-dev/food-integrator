import logging
import sys
from datetime import datetime

import structlog
from src.config import settings

def setup_logger():
    """Configura o structlog para toda a aplicação."""
    
    # Define o formato de saída baseado no ambiente
    if settings.app_env == "production":
        # JSON puro para Dozzle / Datadog / ELK
        renderer = structlog.processors.JSONRenderer()
    else:
        # Texto colorido e amigável para o terminal de desenvolvimento
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars, # Permite injetar variáveis (ex: correlation_id)
            structlog.stdlib.add_log_level,          # Adiciona "level": "info/error"
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"), # Adiciona timestamp ISO 8601
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,    # Formata stack traces de erros
            structlog.processors.UnicodeDecoder(),
            renderer,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configura o logging padrão do Python para passar pelo structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
    )

    return structlog.get_logger()

# Instância global do logger
logger = setup_logger()
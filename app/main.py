"""
Healthcare Claims RAG Engine — FastAPI Application Entry Point.

Startup sequence:
  1. Configure structured logging
  2. Attempt to load the policy index from the vector store
  3. Register API routes

n8n webhook target: POST http://localhost:8000/verify-claim
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import claims, health
from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.rag import indexer, retriever

configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load the policy knowledge base index at startup.
    If the index doesn't exist yet, the /verify-claim endpoint will return 503
    with instructions to run scripts/ingest_policies.py.
    """
    logger.info(
        "startup",
        llm_model=settings.llm_model,
        vector_store=settings.vector_store_type,
    )
    try:
        index = indexer.load_index()
        retriever.set_index(index)
        logger.info("policy_index_loaded_successfully")
    except Exception as exc:
        logger.warning(
            "policy_index_not_loaded",
            reason=str(exc),
            action="Run: python scripts/ingest_policies.py",
        )

    yield  # Application runs here

    logger.info("shutdown")


app = FastAPI(
    title="Healthcare Claims RAG Engine",
    description=(
        "RAG-based engine for verifying medical claims against CMS LCD/NCD policies. "
        "Returns structured Approve / Deny / Appeal decisions with appeal letters."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# Register routers
app.include_router(health.router)
app.include_router(claims.router)


# ── Dev entrypoint ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.app_debug,
        log_level="debug" if settings.app_debug else "info",
    )

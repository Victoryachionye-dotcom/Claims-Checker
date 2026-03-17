"""Health check endpoint — used by n8n and load balancers to verify the service is live."""

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.config import settings
from app.rag.retriever import _index

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    version: str
    llm_model: str
    vector_store: str
    index_loaded: bool


@router.get("/health", response_model=HealthResponse, tags=["System"])
def health_check() -> HealthResponse:
    """
    Returns service health status.
    index_loaded = False means ingest_policies.py has not been run yet.
    """
    return HealthResponse(
        status="ok",
        version="0.1.0",
        llm_model=settings.llm_model,
        vector_store=settings.vector_store_type,
        index_loaded=_index is not None,
    )

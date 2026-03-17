"""
Application settings loaded from .env via pydantic-settings.
All vector store types are toggled here — swap chroma ↔ qdrant with one .env change.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── API Keys ──────────────────────────────────────────────────────────────
    anthropic_api_key: str
    openai_api_key: str

    # ── LLM ──────────────────────────────────────────────────────────────────
    llm_model: str = "claude-sonnet-4-6"
    llm_max_tokens: int = 4096

    # ── Embeddings ────────────────────────────────────────────────────────────
    embedding_model: str = "text-embedding-3-small"

    # ── Vector Store ──────────────────────────────────────────────────────────
    # "chroma" → local ChromaDB (default, zero-friction)
    # "qdrant" → persistent Qdrant (requires docker-compose up -d)
    vector_store_type: str = "chroma"
    chroma_persist_dir: str = "./data/vector_store"
    qdrant_url: str = "http://localhost:6333"
    qdrant_collection: str = "healthcare_policies"

    # ── Knowledge Base ────────────────────────────────────────────────────────
    policy_kb_dir: str = "./data/policy_kb"

    # ── FastAPI ───────────────────────────────────────────────────────────────
    app_debug: bool = False
    app_host: str = "0.0.0.0"
    app_port: int = 8000


settings = Settings()

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_key_prefix: str = "harness"
    redis_ttl_seconds: int = 3600

    # PostgreSQL
    postgres_dsn: str = "postgresql://harness:harness@localhost:5432/harness"

    # Escalation
    escalation_webhook_url: str | None = None   # Slack / PagerDuty URL

    # Monitoring
    metrics_port: int = 9090

    # LLM (для evals)
    anthropic_api_key: str = ""

    # OpenRouter (LLM-судья для evals)
    # Имя поля openrouter_api_key само совпадает с OPENROUTER_API_KEY в .env.
    # Для BASE_URL и BASE_MODEL задаём псевдонимы (validation_alias).
    openrouter_api_key: str = ""
    openrouter_base_url: str = Field(
        default="https://openrouter.ai/api/v1",
        validation_alias="BASE_URL",
    )
    judge_model: str = Field(
        default="google/gemini-2.5-flash",
        validation_alias="BASE_MODEL",
    )


settings = Settings()
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


settings = Settings()
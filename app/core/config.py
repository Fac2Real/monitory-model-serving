"""
app.core.config
---------------
.env 기반 런타임 설정 (Pydantic BaseSettings)
"""

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ───── AWS & S3 ─────
    AWS_REGION: str = Field(default="ap-northeast-2", alias="AWS_REGION")
    AWS_ACCESS_KEY_ID: str | None = Field(default=None, alias="AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: str | None = Field(default=None, alias="AWS_SECRET_ACCESS_KEY")

    S3_MODEL_BUCKET_NAME: str = Field(default="monitory-model", alias="S3_MODEL_BUCKET_NAME")
    S3_MODEL_KEY: str = Field(default="models/latest/lgbm_regressor.json", alias="S3_MODEL_KEY")

    S3_INPUT_DATA_BUCKET_NAME: str = Field(default="monitory-bucket", alias="S3_INPUT_DATA_BUCKET_NAME")
    S3_INPUT_DATA_KEY: str = Field(default="EQUIPMENT/", alias="S3_INPUT_DATA_KEY")

    # ───── 로깅 ─────
    LOG_LEVEL: str = Field(default="INFO", alias="LOG_LEVEL")
    LOG_FORMAT: str = Field(default="TEXT", alias="LOG_FORMAT")        # TEXT / JSON
    LOG_EMOJI: bool = Field(default=True, alias="LOG_EMOJI")

    # Prometheus 사용 여부 등 추가 가능
    prometheus_enabled: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",         # 알 수 없는 환경변수 무시
    )


@lru_cache
def get_settings() -> Settings:
    """FastAPI dependency 또는 일반 import 에서 호출"""
    return Settings()


settings = get_settings()   # 전역 싱글턴

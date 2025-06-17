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
    aws_region: str = Field(default="ap-northeast-2", alias="AWS_REGION")
    aws_access_key_id: str | None = Field(default=None, alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str | None = Field(default=None, alias="AWS_SECRET_ACCESS_KEY")

    s3_model_bucket: str = Field(default="monitory-model", alias="S3_MODEL_BUCKET_NAME")
    s3_model_key: str = Field(default="models/latest/lgbm_regressor.json", alias="S3_MODEL_KEY")

    s3_input_data_bucket: str = Field(default="monitory-bucket", alias="S3_INPUT_DATA_BUCKET_NAME")
    s3_input_data_key: str = Field(default="EQUIPMENT/", alias="S3_INPUT_DATA_KEY")

    # ───── 로깅 ─────
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="TEXT", alias="LOG_FORMAT")        # TEXT / JSON
    log_emoji: bool = Field(default=True, alias="LOG_EMOJI")

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

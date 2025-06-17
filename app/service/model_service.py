"""
Model Service
==============

• LightGBM Booster를 S3에서 가져와 메모리에 캐싱
• 예측 요청이 들어오면 캐싱된 모델로 추론 수행
• 모든 STDOUT → logging 로 대체 (Argo/K8s 로그 수집용)

환경변수 및 공통 상수는 app.core.* 모듈에서 공급받습니다.
"""
from __future__ import annotations

from io import BytesIO
from typing import Optional

import boto3
import lightgbm as lgb
import pandas as pd
from botocore.exceptions import ClientError

from app.core.config import settings          # Pydantic BaseSettings instance
from app.core.constants import FEATURE_COLS   # 학습에 사용한 컬럼 리스트
from app.core.logging_config import get_logger

logger = get_logger("monitory.model")

# ────────────────────────────────────────────────────────────
# 전역 모델 인스턴스
# ────────────────────────────────────────────────────────────
_model: Optional[lgb.Booster] = None


# ────────────────────────────────────────────────────────────
# S3 헬퍼
# ────────────────────────────────────────────────────────────
def _get_s3_client():
    """
    Boto3 S3 클라이언트를 생성합니다.

    • IAM Role/EKS IRSA 등을 사용할 경우 access_key 없이 호출해도 무방합니다.
    """
    if settings.AWS_ACCESS_KEY_ID and settings.AWS_SECRET_ACCESS_KEY:
        logger.debug("S3: key/secret 기반 인증 사용")
        return boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION,
        )
    logger.debug("S3: IAM Role 기반 인증 사용")
    return boto3.client("s3", region_name=settings.AWS_REGION)


# ────────────────────────────────────────────────────────────
# 모델 로드 / 캐싱
# ────────────────────────────────────────────────────────────
def _load_model_from_s3() -> Optional[lgb.Booster]:
    """S3에서 모델을 다운로드해 전역 변수에 로드합니다."""
    global _model

    if _model is not None:
        logger.info("⭐️  모델이 이미 메모리에 로드되어 있습니다.")
        return _model

    bucket = settings.S3_MODEL_BUCKET_NAME
    key = settings.S3_MODEL_KEY

    if not bucket or not key:
        logger.error("❌ S3 모델 경로가 설정되지 않았습니다.")
        return None

    logger.info(f"💡 모델 다운로드: s3://{bucket}/{key}")
    s3 = _get_s3_client()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        model_str = obj["Body"].read().decode("utf-8")
        _model = lgb.Booster(model_str=model_str)
        logger.info("✅ 모델 로드 성공")
    except ClientError as e:
        logger.exception(f"🚨 S3 ClientError: {e}")
        _model = None
    except Exception as e:
        logger.exception(f"🚨 모델 로딩 오류: {e}")
        _model = None

    return _model


def get_model() -> Optional[lgb.Booster]:
    """
    캐시된 모델을 반환합니다.
    필요 시 `_load_model_from_s3()`를 자동 호출해 캐싱합니다.
    """
    if _model is None:
        logger.debug("🔄 캐시 미존재 → S3 로드 시도")
        _load_model_from_s3()
    return _model


def is_ready() -> bool:
    """헬스체크용 헬퍼: 모델이 메모리에 올라왔는지 여부."""
    return get_model() is not None


# ────────────────────────────────────────────────────────────
# 예측
# ────────────────────────────────────────────────────────────
def predict(df_wide: pd.DataFrame) -> Optional[list[float]]:
    """
    Parameters
    ----------
    df_wide : pd.DataFrame
        전처리·피벗 완료된 Wide 형태 입력

    Returns
    -------
    list[float] | None
        1-D 예측 결과. 실패 시 `None`.
    """
    model = get_model()
    if model is None:
        logger.error("❌ [predict] 모델이 로드되지 않아 예측할 수 없습니다.")
        return None

    if df_wide is None or df_wide.empty:
        logger.error("❌ [predict] 입력 데이터가 비어 있거나 로드 실패.")
        return None

    # LightGBM 입력 구성
    if "equipment" in df_wide.columns:
        df_wide["equipment"] = df_wide["equipment"].astype("category")

    num_cols = [c for c in FEATURE_COLS if c != "equipment"]
    X = df_wide[num_cols].fillna(0)
    X["equipment"] = df_wide["equipment"]

    logger.info(f"✅ [predict] 모델 입력 shape={X.shape}")

    try:
        y_pred = model.predict(X)
        logger.info(f"✅ [predict] 예측 완료 → {y_pred.tolist()}")
        return y_pred.tolist()
    except Exception as e:
        logger.exception(f"🚨 [predict] 예측 오류: {e}")
        return None

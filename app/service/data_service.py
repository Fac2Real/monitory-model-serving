"""
Data Service
============

• S3 → 최신 1 시간 센서 JSON(또는 JSONL) → DataFrame → 전처리 → 모델 입력용 wide 포맷 반환
• 로그는 print 대신 logger 사용
• 설정‧상수는 app.core 모듈에서 가져와 model_service 와 컬럼 싱크 유지
"""
from __future__ import annotations

import io
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from app.core.config import settings                  # Pydantic BaseSettings
from app.core.constants import FEATURE_COLS           # 모델 학습 컬럼
from app.core.logging_config import get_logger

logger = get_logger("monitory.data")


# ────────────────────────────────────────────────────────────
# S3 헬퍼
# ────────────────────────────────────────────────────────────
def _get_s3_client():
    """IAM Role 혹은 Key/Secret 방식 둘 다 지원"""
    if settings.AWS_ACCESS_KEY_ID and settings.AWS_SECRET_ACCESS_KEY:
        logger.debug("S3: key/secret 기반 인증")
        return boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION,
        )
    logger.debug("S3: IAM Role 기반 인증")
    return boto3.client("s3", region_name=settings.AWS_REGION)


def _get_s3_key_for_input(zone_id: str, equip_id: str) -> str:
    """equipId·zoneId 기준 ‘1 시간 전’ 날짜 디렉터리 생성"""
    one_hour_ago = datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(hours=1)
    date = one_hour_ago.strftime("%Y-%m-%d")
    key = f"EQUIPMENT/date={date}/zone_id={zone_id}/equip_id={equip_id}/"
    logger.info(f"✅ S3 Key 생성: date={date}, zoneId={zone_id}, equipId={equip_id}")
    return key


# ────────────────────────────────────────────────────────────
# 데이터 로드
# ────────────────────────────────────────────────────────────
def load_input_data_from_s3(zone_id: str, equip_id: str) -> Optional[pd.DataFrame]:
    """
    S3에서 가장 최신 JSON(.json / .jsonl) 파일을 읽어 전처리 결과(DataFrame) 반환.
    실패 시 `None`.
    """
    bucket = settings.S3_INPUT_DATA_BUCKET_NAME
    prefix = _get_s3_key_for_input(zone_id, equip_id)

    if not bucket or not prefix:
        logger.error("❌ S3 입력 버킷/키가 설정되지 않았습니다.")
        return None

    s3 = _get_s3_client()
    latest_key = None
    latest_time = None

    try:
        logger.info(f"💡 객체 나열: s3://{bucket}/{prefix}")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in resp:
            logger.error(f"❌ 경로 없음: s3://{bucket}/{prefix}")
            return None

        for obj in resp["Contents"]:
            key = obj["Key"]
            if key == prefix or not key.endswith(".json"):
                continue
            mod_time = obj["LastModified"]
            if latest_time is None or mod_time > latest_time:
                latest_key, latest_time = key, mod_time

        if latest_key is None:
            logger.error(f"❌ '.json' 파일 없음: s3://{bucket}/{prefix}")
            return None

        logger.info(f"⭐️ 최신 파일: s3://{bucket}/{latest_key} (수정: {latest_time})")
        file_obj = s3.get_object(Bucket=bucket, Key=latest_key)
        content = file_obj["Body"].read().decode("utf-8")

        # JSONL ↔ JSON 자동 판별
        if "\n" in content.strip():
            df_raw = pd.read_json(io.StringIO(content), lines=True)
        else:
            df_raw = pd.read_json(io.StringIO(content), orient="records")

        logger.info(f"📊 원본 DF shape={df_raw.shape}")
        return preprocess_input_data(df_raw, window=5)

    except ClientError as e:
        logger.exception(f"🚨 S3 ClientError: {e}")
        return None
    except Exception as e:
        logger.exception(f"🚨 S3 데이터 로드 오류: {e}")
        return None


# ────────────────────────────────────────────────────────────
# 전처리
# ────────────────────────────────────────────────────────────
def preprocess_input_data(df: pd.DataFrame, window: int = 5) -> Optional[pd.DataFrame]:
    """
    • rolling mean/std, pivot, 센서 누락컬럼 보정, power_factor 생성
    • 반환 컬럼 = FEATURE_COLS (constants.py) 과 100% 일치
    """
    if df is None or df.empty:
        logger.error("❌ 입력 데이터 없음")
        return None

    logger.info("📊 [1] 시간순 정렬")
    df = df.sort_values(["equipId", "sensorType", "time"])

    logger.info("📊 [2] rolling 계산")
    df["val_rollmean"] = (
        df.groupby(["equipId", "sensorType"])["val"]
        .rolling(window=window, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )
    df["val_rollstd"] = (
        df.groupby(["equipId", "sensorType"])["val"]
        .rolling(window=window, min_periods=1)
        .std()
        .reset_index(level=[0, 1], drop=True)
    )

    logger.info("📊 [3] sensorType 매핑 및 필터링")
    mapping = {
        "temp": "temperature",
        "humid": "humidity",
        "pressure": "pressure",
        "vibration": "vibration",
        "reactive_power": "reactive_power",
        "active_power": "active_power",
    }
    df = df[df["sensorType"].isin(mapping.keys())]

    logger.info("📊 [4] 그룹 집계(mean)")
    agg = (
        df.groupby(["equipId", "sensorType"])[["val", "val_rollmean", "val_rollstd"]]
        .mean()
        .reset_index()
    )

    logger.info("📊 [5] pivot → wide")
    wide = agg.pivot(
        index=["equipId"],
        columns="sensorType",
        values=["val", "val_rollmean", "val_rollstd"],
    ).reset_index()

    logger.info("📊 [6] 컬럼명 평탄화")
    wide.columns = [
        col[0]
        if col[0] == "equipId"
        else (
            f"{mapping[col[1]]}"
            if col[0] == "val"
            else f"{mapping[col[1]]}_{col[0].replace('val_', '')}"
        )
        for col in wide.columns
    ]
    wide = wide.rename(columns={"equipId": "equipment"})

    logger.info("📊 [7] 누락 센서 컬럼 보정")
    for col in FEATURE_COLS:
        if col not in wide.columns:
            wide[col] = 0
            logger.warning(f"⚠️  누락 컬럼 채움 → {col}")

    logger.info("📊 [8] power_factor 생성")
    wide["power_factor"] = (
        wide["active_power"]
        / (wide["active_power"] ** 2 + wide["reactive_power"] ** 2) ** 0.5
    ).fillna(0)

    logger.info(f"⚠️ power_factor 생성 -> {wide['power_factor']}")

    clean_cols = ["equipment", *[c for c in FEATURE_COLS if c != "equipment"]]
    wide_clean = wide[clean_cols]

    # 👀 미리보기 5행만 로그로 남기기
    logger.info(
        "✅ 전처리 완료! shape=%s\n%s",
        wide_clean.shape,
        wide_clean.head().to_string(index=False)
    )

    return wide_clean

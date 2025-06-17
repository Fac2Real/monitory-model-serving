"""retrain_service.py
모델 재학습 · 버저닝 · S3 업로드 서비스
-------------------------------------------------
· 데이터 적재/전처리 :  app.services.data_service
· 공통 상수           :  app.core.constants
· 런타임 설정         :  app.core.config.settings

Usage
-----
from app.services import retrain_service
result = retrain_service.train_and_upload()
print(result)
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

import boto3
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import (mean_absolute_error, mean_squared_error,
                             r2_score)
from sklearn.model_selection import train_test_split

from app.core.config import settings
from app.core import constants
from app.service import data_service
from app.core.logging_config import get_logger

from app.core import constants as C
# ───────────────────────────────────────────────────────────────────────────────
# 로깅 설정
# ───────────────────────────────────────────────────────────────────────────────
logger = get_logger("monitory.retrain")

# ───────────────────────────────────────────────────────────────────────────────
# S3 helpers
# ───────────────────────────────────────────────────────────────────────────────
def _get_s3_client():
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


# ───────────────────────────────────────────────────────────────────────────────
# Raw 데이터 적재
# ───────────────────────────────────────────────────────────────────────────────
def _list_object_keys(bucket: str, prefix: str) -> List[str]:
    s3 = _get_s3_client()
    logger.debug("🔎 S3 list prefix=%s", prefix)
    paginator = s3.get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)
    logger.info("🗂️  %s개 JSON (bucket=%s, prefix=%s)", len(keys), bucket, prefix)
    return keys


def _load_ndjson(keys: List[str], bucket: str, sample_n: Optional[int] = None) -> pd.DataFrame:
    s3 = _get_s3_client()
    dfs: List[pd.DataFrame] = []
    iterable = keys if sample_n is None else keys[:sample_n]
    for k in iterable:
        body = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
        dfs.append(pd.read_json(io.BytesIO(body), lines=True))
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    logger.info("📥 concat → %s rows", len(df))
    return df

# ───────────────────────────────────────────────────────────────────────────────
# Raw ➜ df_wide + 전처리 & RUL (Colab 로직)
# ───────────────────────────────────────────────────────────────────────────────
_DOWNSTREAM_WIN = 5
_MAX_RUL = 30


def _prepare_training_df(df_raw: pd.DataFrame, win: int = _DOWNSTREAM_WIN) -> pd.DataFrame:
    """Raw NDJSON → wide + rolling + RUL 계산"""
    if df_raw.empty:
        logger.error("raw_df empty → 전처리 중단")
        return pd.DataFrame()

    logger.info("🔰 raw rows=%s", len(df_raw))
    df = df_raw.copy()

    # timestamp 파싱 & 불필요 col 제거
    df["timestamp"] = pd.to_datetime(df["time"], utc=True)
    df.drop(columns=["time", "zoneId", "sensorId"], inplace=True, errors="ignore")

    # 센서 필터링
    keep = [
        "active_power", "humid", "pressure", "reactive_power", "temp", "vibration"
    ]
    df_use = df[df["sensorType"].isin(keep)].copy()

    # 1시간 floor
    df_use["ts_hour"] = df_use["timestamp"].dt.floor("h")

    # pivot
    df_wide = (
        df_use.pivot_table(
            index=["ts_hour", "equipId"],
            columns="sensorType",
            values="val",
            aggfunc="max",
        )
        .reset_index()
        .rename(columns={"ts_hour": "timestamp"})
    )

    # rolling feature
    num_cols = [
        "temp", "pressure", "vibration", "humid", "active_power", "reactive_power"
    ]
    for col in num_cols:
        grp = df_wide.groupby("equipId")[col]
        df_wide[f"{col}_rollmean"] = grp.rolling(win, 1).mean().reset_index(level=0, drop=True)
        df_wide[f"{col}_rollstd"] = grp.rolling(win, 1).std().reset_index(level=0, drop=True).fillna(0)

    df_wide["power_factor"] = (
        df_wide["active_power"] / np.sqrt(df_wide["active_power"] ** 2 + df_wide["reactive_power"] ** 2)
    ).fillna(0)

    # 결측 보정
    raw_cols = ["active_power", "reactive_power", "temp", "pressure", "vibration", "humid"]
    roll_std = [c for c in df_wide.columns if c.endswith("_rollstd")]

    df_wide = df_wide.sort_values(["equipId", "timestamp"])
    df_wide[raw_cols] = df_wide.groupby("equipId")[raw_cols].ffill(limit = 3).bfill(limit = 1)
    for c in raw_cols:
        df_wide[c] = df_wide[c].fillna(df_wide[c].median())
    df_wide[roll_std] = df_wide[roll_std].fillna(0)

    # faulty & RUL
    df_use["alert"] = 0
    for sensor, (lo, hi) in constants.ALERT_THRESH.items():
        m = df_use["sensorType"] == sensor
        vals = df_use.loc[m, "val"]
        df_use.loc[m, "alert"] = ((vals < lo) | (vals > hi)).astype("int8")

    faulty = (
        df_use.groupby(["ts_hour", "equipId"])["alert"].sum().reset_index(name="cnt")
    )
    faulty["faulty"] = (faulty["cnt"] >= 2).astype(int)

    df_wide = df_wide.merge(
        faulty.rename(columns={"ts_hour": "timestamp"})[["timestamp", "equipId", "faulty"]],
        on=["timestamp", "equipId"],
        how="left",
    ).fillna({"faulty": 0}).astype({"faulty": "int8"})

    def _add_rul(g: pd.DataFrame) -> pd.DataFrame:
        n = len(g)
        rul = np.full(n, np.nan, dtype=np.float32)
        nxt: int | None = None
        for i in range(n - 1, -1, -1):
            if g.iloc[i]["faulty"] == 1:
                nxt = 0
            else:
                nxt = (nxt + 1) if nxt is not None else np.nan
            rul[i] = nxt
        g["rul"] = rul
        return g

    df_wide = df_wide.sort_values(["equipId", "timestamp"]).groupby("equipId", group_keys=False).apply(_add_rul)
    df_wide["rul"] = df_wide["rul"].fillna(_MAX_RUL).clip(upper=_MAX_RUL)

    # ── 컬럼명 정규화 ─────────────────────────────────────────────
    # 1) equipId → equipment
    if "equipId" in df_wide.columns:
        df_wide = df_wide.rename(columns={"equipId": "equipment"})
        logger.debug("🔧 rename equipId → equipment 완료")

        # 2) 센서명 표준화  (temp→temperature, humid→humidity)
    sensor_map = {"temp": "temperature", "humid": "humidity"}
    rename_dict = {}

    for old, new in sensor_map.items():
        # base 컬럼
        if old in df_wide.columns:
            rename_dict[old] = new
        # rolling mean / std
        if f"{old}_rollmean" in df_wide.columns:
            rename_dict[f"{old}_rollmean"] = f"{new}_rollmean"
        if f"{old}_rollstd" in df_wide.columns:
            rename_dict[f"{old}_rollstd"] = f"{new}_rollstd"

    if rename_dict:
        df_wide = df_wide.rename(columns=rename_dict)
        logger.debug("🔧 sensor 컬럼명 표준화 완료: %s", rename_dict)

    logger.info("✅ 전처리 완료 → shape=%s", df_wide.shape)
    return df_wide

# ───────────────────────────────────────────────────────────────────────────────
# 데이터 밸런싱 (Colab 로직 그대로)
# ───────────────────────────────────────────────────────────────────────────────
DOWN_RATIO_ZERO = C.DOWN_RATIO_ZERO
OVER_RATIO      = C.OVER_RATIO

def _balance_rul(df: pd.DataFrame) -> pd.DataFrame:
    zero_df = df[df.rul == 0].sample(frac=DOWN_RATIO_ZERO, random_state=42)
    dfs = [zero_df]
    for k, v in OVER_RATIO.items():
        tmp = df[df.rul == k]
        if not tmp.empty:
            dfs.append(pd.concat([tmp] * v, ignore_index=True))
    balanced = pd.concat(dfs, ignore_index=True).sample(frac=1, random_state=42)
    logger.info("⚖️  balance → %s rows (down 0, over 1‒15)", len(balanced))
    return balanced


# ───────────────────────────────────────────────────────────────────────────────
# 학습 로직
# ───────────────────────────────────────────────────────────────────────────────
_FEATURE_COLS = constants.FEATURE_COLS
_TARGET_COL = "rul"


def _train_model(df: pd.DataFrame) -> Tuple[lgb.LGBMRegressor, dict]:
    df["equipment"] = df["equipment"].astype("category")
    X = df[_FEATURE_COLS]
    y = df[_TARGET_COL]

    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    X_train, X_valid, y_train, y_valid = train_test_split(
        X_temp, y_temp, test_size=0.25, random_state=42, stratify=y_temp
    )

    model = lgb.LGBMRegressor(
        n_estimators=600,
        learning_rate=0.05,
        num_leaves=64,
        objective="regression",
        random_state=42,
        verbosity = -1,
    )

    logger.info("🚀 LightGBM fit 시작 (train=%s, valid=%s)", len(X_train), len(X_valid))
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_train, y_train), (X_valid, y_valid)],
        eval_names=["train", "valid"],
        eval_metric="rmse",
        categorical_feature=["equipment"],
        callbacks=[
            lgb.log_evaluation(period=0),
            lgb.early_stopping(50),
            lgb.reset_parameter({"verbose": -1})  # ← 핵심 수정
        ],
    )

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = float(np.sqrt(mse))
    mae = float(mean_absolute_error(y_test, y_pred))
    r2 = float(r2_score(y_test, y_pred))
    logger.info("✅ New Eval | RMSE=%.3f MAE=%.3f R2=%.4f", rmse, mae, r2)

    return model, {"rmse": rmse, "mae": mae, "r2": r2}


# ───────────────────────────────────────────────────────────────────────────────
# 모델 저장 & 버전 관리
# ───────────────────────────────────────────────────────────────────────────────

_MODEL_BUCKET = settings.s3_model_bucket
_LATEST_MODEL_KEY = "models/latest/lgbm_regressor.json"
_LATEST_METRIC_KEY = "models/latest/metrics.json"
_VERSION_TEMPLATE = "models/{:%Y/%m/%d/%H%M%S}/{}"  # dt, filename


def _fetch_latest_rmse() -> Tuple[float, dict]:
    s3 = _get_s3_client()
    try:
        meta = json.loads(s3.get_object(Bucket=_MODEL_BUCKET, Key=_LATEST_METRIC_KEY)["Body"].read())
        return meta.get("rmse", float("inf")), meta
    except s3.exceptions.NoSuchKey:
        logger.warning("🆕 최초 학습 (latest 메트릭 없음)")
        return float("inf"), {}
    except Exception as e:
        logger.error("메트릭 로드 실패: %s", e)
        return float("inf"), {}


def _upload(version_key: str, model_dict: dict, metrics: dict, promote: bool):
    s3 = _get_s3_client()
    # 버전 히스토리 저장
    s3.put_object(Bucket=_MODEL_BUCKET, Key=version_key + "/lgbm_regressor.json", Body=json.dumps(model_dict).encode())
    s3.put_object(Bucket=_MODEL_BUCKET, Key=version_key + "/metrics.json", Body=json.dumps(metrics).encode())

    if promote:
        s3.put_object(Bucket=_MODEL_BUCKET, Key=_LATEST_MODEL_KEY, Body=json.dumps(model_dict).encode())
        s3.put_object(Bucket=_MODEL_BUCKET, Key=_LATEST_METRIC_KEY, Body=json.dumps(metrics).encode())
        logger.info("🏆 최신 모델 승격 → %s", _LATEST_MODEL_KEY)


# ───────────────────────────────────────────────────────────────────────────────
# Public 엔트리포인트 [수정] 날짜 범위 지원
# ───────────────────────────────────────────────────────────────────────────────
MIN_BALANCED_ROWS = C.MIN_BALANCED_ROWS      # 원하는 최소 행 수

def train_and_upload(
    start_day: str | None = None,   # YYYY-MM-DD
    end_day:   str | None = None,   # YYYY-MM-DD
    sample_n:  int  | None = None   # NDJSON 일부만 읽고 싶을 때
) -> dict:
    """
    날짜 범위를 넘기면 day-prefix 모드,
    둘 다 None 이면 월-prefix 모드(기존 방식).
    """

    bucket = settings.s3_input_data_bucket
    keys: list[str] = []

    # ① 일자 범위 ────────────────────────────────────────
    if start_day and end_day:
        day = datetime.strptime(start_day, "%Y-%m-%d")
        end = datetime.strptime(end_day, "%Y-%m-%d")
        while day <= end:
            p = f"EQUIPMENT/date={day.strftime('%Y-%m-%d')}"
            keys.extend(_list_object_keys(bucket, p))
            day += timedelta(days=1)

        # ② 월 단위(레거시) ───────────────────────────────────
    else:
        # 아무 파라미터도 없으면 “오늘” 기준 월
        month_key = (start_day or end_day or datetime.utcnow().strftime("%Y-%m"))[:7]
        p = f"EQUIPMENT/date={month_key}"
        keys = _list_object_keys(bucket, p)

    if not keys:
        msg = f"S3 데이터 없음 (prefix={p})"
        logger.error(msg)
        return {"status": "error", "msg": msg}

    logger.info("🔢 총 keys=%s (S3 objects to load)", len(keys))

    raw_df = _load_ndjson(keys, bucket, sample_n)
    if raw_df.empty:
        msg = "데이터 로드 실패 (empty df)"
        logger.error(msg)
        return {"status": "error", "msg": msg}

    # 2) 전처리 & RUL -----------------------------------------------------------------
    df_wide = _prepare_training_df(raw_df)
    if df_wide is None or df_wide.empty:
        msg = "전처리 실패"
        logger.error(msg)
        return {"status": "error", "msg": msg}

    balanced_df = _balance_rul(df_wide)

    if len(balanced_df) < MIN_BALANCED_ROWS:
        msg = f"balanced rows {len(balanced_df)} < {MIN_BALANCED_ROWS}"
        logger.warning("⏭️  %s → 재학습 Skip", msg)
        return {"status": "skip", "reason": "too_few_rows", "rows": len(balanced_df)}

    # 3) 학습 -------------------------------------------------------------------------
    try:
        model, metrics = _train_model(balanced_df)
    except Exception as e:
        logger.exception("💥 Train failed: %s", e)
        return {"status": "error", "reason": "train_failed", "msg": str(e)}

    # 4) 메트릭 비교 & 저장 ------------------------------------------------------------
    MIN_R2 = 0.20  # 기준치, config/constant 로 빼두기
    old_rmse, old_meta = _fetch_latest_rmse()

    promote = (metrics["rmse"] < old_rmse) and (metrics["r2"] >= MIN_R2)

    # 버전 경로 (디렉터리)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    version_dir = _VERSION_TEMPLATE.format(now, "").rstrip("/")


    logger.info("✅ Old Eval | RMSE=%.3f ", old_rmse)
    _upload(version_dir, model.booster_.dump_model(), metrics, promote)

    logger.info("📤 S3 업로드 완료 | promote=%s | version_dir=%s", promote, version_dir)

    return {
        "status": "ok",
        "trained_on": len(balanced_df),
        "metrics": metrics,
        "promoted": promote,
        "version_dir": version_dir,
        "prev_rmse": old_rmse if old_rmse != float("inf") else None,
    }
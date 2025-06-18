"""app/scheduler.py
FastAPI 프로세스 내부에서 동작하는 BackgroundScheduler.
매일 자정(KST)에 재학습용 파이프라인을 실행합니다.

* 멀티 리플리카 환경에서는 Redis/DynamoDB 락 또는 SQS FIFO 로크를 추가해 동시 실행을 방지하세요.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.service.retrain_service import train_and_upload
from app.core.logging_config import get_logger
from app.core.config import settings
from app.core import constants

logger = get_logger("monitory.scheduler")

# ───────────────────────────────────────────────────────────────────────────────
# 간단한 S3 행(row) 수 카운트 유틸 (prefix 범위)
# ───────────────────────────────────────────────────────────────────────────────
import boto3
def _count_rows_in_s3_range(start_day: str, end_day: str) -> int:
    """날짜 YYYY-MM-DD 범위의 NDJSON line 개수 합산 (빠른 추정용)."""
    s3 = boto3.client(
        "s3",
        region_name=settings.AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )
    bucket = settings.S3_INPUT_DATA_BUCKET_NAME
    total = 0
    current = datetime.strptime(start_day, "%Y-%m-%d")
    end     = datetime.strptime(end_day, "%Y-%m-%d")
    while current <= end:
        prefix = f"EQUIPMENT/date={current.strftime('%Y-%m-%d')}"
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in resp.get("Contents", []):
            total += obj["Size"] // 200  # NDJSON 1줄≈200B 로 러프하게 추정
        current += timedelta(days=1)
    return total

# ───────────────────────────────────────────────────────────────────────────────
# 메인 잡 함수
# ───────────────────────────────────────────────────────────────────────────────
MIN_ROWS = 50_000   # 충분성 기준 (constants.py 로 옮겨도 됨)
MIN_R2   = 0.20     # 승격 기준 (retrain_service 내부에서도 참조)

def run_retrain_job() -> None:
    tz_kst = timezone(timedelta(hours=9))
    today  = datetime.now(tz_kst)
    start_day = (today - timedelta(days=21)).strftime("%Y-%m-%d")
    end_day   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    month_key = (today - timedelta(days=1)).strftime("%Y-%m")  # 어제 날짜 기준 월

    logger.info("📆 Retrain job start | range=%s~%s", start_day, end_day)

    rows = _count_rows_in_s3_range(start_day, end_day)
    if rows < MIN_ROWS:
        logger.warning("⏭️ 데이터 부족 (%s rows < %s) → Skip retrain", rows, MIN_ROWS)
        return

    try:
        result = train_and_upload(start_day=start_day, end_day=end_day)
        logger.info("🎯 Retrain result: %s", result)
    except Exception as e:
        logger.exception("💥 Retrain job crashed: %s", e)
        result = {"status": "error", "reason": "exception", "msg": str(e)}

# ───────────────────────────────────────────────────────────────────────────────
# Scheduler 인스턴스 (FastAPI 앱에서 import 해서 start)
# ───────────────────────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler(timezone="Asia/Seoul")
cron = CronTrigger(hour=0, minute=0, second=0, timezone="Asia/Seoul")
scheduler.add_job(run_retrain_job, cron, id="daily_retrain", replace_existing=True)
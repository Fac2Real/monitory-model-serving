"""tasks/retrain.py
CLI 테스트용 재학습 트리거 스크립트.

예)
$ python -m tasks.retrain --month 2025-06 --sample 500
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

# FastAPI 애플리케이션 패키지를 import 경로에 추가 (스탠드얼론 실행 대비)
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from app.service.retrain_service import train_and_upload


def main():
    parser = argparse.ArgumentParser(description="Run model retraining manually.")
    parser.add_argument(
        "--month",
        "-m",
        type=str,
        default=datetime.utcnow().strftime("%Y-%m"),
        help="Target month (YYYY-MM). Defaults to current UTC month.",
    )
    parser.add_argument(
        "--sample",
        "-s",
        type=int,
        default=None,
        help="Sample n files for quick test. Omit for full dataset.",
    )
    args = parser.parse_args()

    print(f"🛠️  Manual retrain start | month={args.month} sample_n={args.sample}")

    result = train_and_upload(target_month=args.month, sample_n=args.sample)

    status = result.get("status")
    if status == "ok":
        print("✅ Retrain finished ✔")
    else:
        print("❌ Retrain error:", result)

    # Pretty-print result JSON
    import json
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
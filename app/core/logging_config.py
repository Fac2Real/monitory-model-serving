"""
app.core.logging_config
-----------------------
애플리케이션 전역 로깅 설정.

• STDOUT 로 출력되므로 Docker/K8s/Argo CD 로그 수집기가 자동으로 읽어 갑니다.
• JSON ↔ 텍스트 포맷 전환, 로그 레벨 등은 환경변수로 제어할 수 있습니다.
"""

import json
import logging
import os
from logging.config import dictConfig
from typing import Any, Dict

# ────────────────────────────────
# 1. 공통 옵션
# ────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "TEXT").upper()         # TEXT or JSON
EMOJI_ON = os.getenv("LOG_EMOJI", "true").lower() == "true"  # 이모티콘 사용 여부

# ────────────────────────────────
# 2. Formatter 정의
# ────────────────────────────────
def _json_formatter(record: logging.LogRecord) -> str:
    base: Dict[str, Any] = {
        "ts": record.created,
        "level": record.levelname,
        "logger": record.name,
        "msg": record.getMessage(),
    }
    return json.dumps(base, ensure_ascii=False)

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        return _json_formatter(record)

TEXT_FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"

# ────────────────────────────────
# 3. dictConfig
# ────────────────────────────────
dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "text": {"format": TEXT_FMT},
        "json": {"()": JsonFormatter},
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "formatter": "json" if LOG_FORMAT == "JSON" else "text",
        }
    },
    "loggers": {
        # 애플리케이션 로거
        "monitory": {
            "handlers": ["stdout"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        # Uvicorn (서버) 로거
        "uvicorn.error": {
            "handlers": ["stdout"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["stdout"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
    },
    # 루트 로거까지 바꾸고 싶으면 주석 해제
    # "root": {"level": LOG_LEVEL, "handlers": ["stdout"]},
})

# ────────────────────────────────
# 4. 편의 함수
# ────────────────────────────────
def get_logger(name: str) -> logging.Logger:
    """
    모듈에서 호출할 때:
        from app.core.logging_config import get_logger
        logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    # 이모티콘 Hook (옵션)
    if EMOJI_ON and not hasattr(logger, "_emoji_patch"):
        def _add_emoji(record: logging.LogRecord) -> bool:
            if record.levelno >= logging.ERROR:
                record.msg = f"❌ {record.msg}"
            elif record.levelno >= logging.WARNING:
                record.msg = f"⚠️  {record.msg}"
            elif record.levelno >= logging.INFO:
                record.msg = f"✅ {record.msg}"
            return True
        logger.addFilter(_add_emoji)  # type: ignore[arg-type]
        logger._emoji_patch = True     # type: ignore[attr-defined]

    return logger

"""
app.core.constants
────────────────────────────────────────────
모델 전처리·학습·추론에 공통으로 쓰이는 하드코딩 상수 모음
"""

# ── 센서 Alert 임계치 ────────────────────────
# (lo, hi) 튜플 — 사양·현장 경험치에 맞게 수정
ALERT_THRESH: dict[str, tuple[float, float]] = {
    "temperature":    (41.0, 101.0),       # °C
    "pressure":       (4.6,  66.88),       # bar
    "vibration":      (-0.5, 3.80),        # g(rms)
    "humidity":       (14.5, 85.54),       # %RH
    "active_power":   (0.0, 168_026.0),    # W
    "reactive_power": (0.0,  86_759.0),    # var
}

# ── 모델 입력 Feature 목록 (학습·추론 공통) ──
FEATURE_COLS: list[str] = [
    # raw 센서
    "temperature", "pressure", "vibration", "humidity",
    "active_power", "reactive_power",
    # rolling mean/std
    "active_power_rollmean",    "active_power_rollstd",
    "reactive_power_rollmean",  "reactive_power_rollstd",
    "temperature_rollmean",     "temperature_rollstd",
    "pressure_rollmean",        "pressure_rollstd",
    "vibration_rollmean",       "vibration_rollstd",
    "humidity_rollmean",        "humidity_rollstd",
    # 파생
    "power_factor",
    # 범주형
    "equipment",
]

# ── 재학습 전용 샘플링 비율 ──
DOWN_RATIO_ZERO: float = 0.20              # rul == 0 구간 다운샘플
OVER_RATIO = {                # 희귀 RUL(1‒15) 구간 배수 ↑
    1: 3, 2: 8, 3: 8,
    4: 10, 5: 10,
    6: 12, 7: 16, 8: 17, 9: 19,
    10: 20, 11: 20, 12: 20, 13: 20, 14: 20, 15: 20
}



MAX_RUL_DAYS: int = 30          # 아직 고장 전 구간 RUL 맥스 (일 기준)
ROLLING_WINDOW: int = 5         # rolling mean/std 기본 윈도우

MIN_ROWS = 50_000   # 충분성 기준 (constants.py 로 옮겨도 됨)
MIN_R2   = 0.20     # 승격 기준 (retrain_service 내부에서도 참조)
MIN_BALANCED_ROWS = 300      # 원하는 최소 행 수

MIN_R2 = 0.20      # 모델 승격 기준치

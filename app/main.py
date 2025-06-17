from fastapi import FastAPI
from datetime import datetime, timedelta
from prometheus_fastapi_instrumentator import Instrumentator
from app.api.v1 import router as api_router
from app.scheduler import scheduler, run_retrain_job
from apscheduler.triggers.date import DateTrigger

app = FastAPI(
    title="Monitory ML Service",
    description=(
        "Monitory ML Service는 스마트 팩토리 설비 센서 데이터를 기반으로 **잔존 수명(RUL) 예측**, "
        "**실시간 이상 탐지**, **모델 성능 메트릭 조회** 기능을 제공합니다. "
        "모든 엔드포인트는 `/api/v1` 하위 경로로 노출되며, Prometheus 지표 수집을 통해 "
        "모델·서비스 상태를 모니터링할 수 있습니다."
    ),
    version="2.0.0"
)

# Prometheus 지표 노출
Instrumentator().instrument(app).expose(app)

#라우트 등록
app.include_router(api_router, prefix="/api/v1")

@app.on_event("startup")
async def startup():
    if not scheduler.running:
        scheduler.start()
        # Optional: 첫 시작 시 5초 후 바로 한번 실행 → 개발·테스트용
        scheduler.add_job(run_retrain_job, DateTrigger(run_date=datetime.now()+timedelta(seconds=5)))
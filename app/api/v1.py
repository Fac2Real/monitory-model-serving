from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from app.service import data_service, model_service, retrain_service
from app.core.logging_config import get_logger

router = APIRouter()
logger = get_logger("monitory.api")       # 레벨·포맷은 logging_config.py가 관리


# ───────────────────────── health ─────────────────────────
@router.get("/health", summary="Health Check")
def health():
    if model_service.is_ready():
        logger.info("✅ HEALTH OK – 모델 로드 완료")
        return {"status": "ok", "message": "API is running and model is loaded."}
    else:
        logger.error("❌ HEALTH ERROR – 모델 미로드")
        raise HTTPException(status_code=503,
                            detail="API is running but MODEL is NOT loaded")

# ───────────────────────── predict ────────────────────────
@router.get("/predict", summary="Predict RUL")
async def predict(zoneId: str, equipId: str):
    logger.info(f"🚀 [predict] 설비 추론 시작: equipId={equipId}, zoneId={zoneId}")

    df = data_service.load_input_data_from_s3(zoneId, equipId)
    if df is None or df.empty:
        logger.warning(f"⚠️  입력 데이터 없음  zoneId={zoneId}, equipId={equipId}")
        raise HTTPException(status_code=404,
                            detail="입력 데이터가 없거나 전처리 결과가 없습니다.")

    preds = model_service.predict(df)
    if preds is None:
        logger.error("❌ 예측 실패")
        raise HTTPException(status_code=500, detail="예측에 실패했습니다.")

    return {"status": "ok", "predictions": preds}   # as-is 와 동일

# ───────────────────────── retrain ────────────────────────
@router.post("/retrain", summary="Trigger model retraining")
async def retrain(background_tasks: BackgroundTasks):
    logger.info("🔄 재학습 백그라운드 작업 등록")
    background_tasks.add_task(retrain_service.train_and_upload)
    return {"status": "ok",
            "msg": "재학습이 백그라운드에서 시작되었습니다."}

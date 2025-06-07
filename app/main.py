from fastapi import FastAPI, HTTPException, Query
from prometheus_fastapi_instrumentator import Instrumentator
from typing import Optional
from . import model
from . import input_data

app = FastAPI(
    title="ML Model Prediction API for Monitory",
    description="Loads a model and input data from S3 to make predictions.",
    version="1.0.0"
)
Instrumentator().instrument(app).expose(app)

# @app.on_event("startup")
# async def startup_event():
#     print("Application startup: Initializing model loader...")
#     model_loader.load_model_from_s3()
    

@app.get("/health", summary="Health Check")
async def health_check():
    # model = model_loader.get_model()
    if model:
        return {"status": "ok", "message": "API is running and model is loaded."}
    else:
        return {"status": "error", "message": "API is running but model is NOT loaded. Check S3 settings or logs."}


# @app.get("/predict-from-s3", response_model=schemas.PredictionResult, summary="Predict using data from S3")
# async def predict(equipId, zoneId):
#     return model.predict_from_s3_data(equipId, zoneId)

# 접속 링크
# http://127.0.0.1:8000/load?equipId=20250507171316-389&zoneId=20250507165750-827
@app.get("/load", summary="Load data from S3")
async def load(zoneId, equipId):
    input_data.load_input_data_from_s3(zoneId=zoneId, equipId=equipId)

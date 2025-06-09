import boto3
import joblib
import os
from dotenv import load_dotenv
from io import BytesIO
import lightgbm as lgb
import pandas as pd

# (input_data.py의 전처리 함수/데이터 불러오기 함수 import)
from app.input_data import preprocess_input_data, load_input_data_from_s3

# 환경변수 로드
load_dotenv()

AWS_REGION = os.getenv('AWS_REGION')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_MODEL_BUCKET = os.getenv('S3_MODEL_BUCKET_NAME', 'monitory-model')
S3_MODEL_KEY = os.getenv('S3_MODEL_KEY', 'models/lgbm_regressor.json')

_model = None  # 전역 모델 인스턴스

def get_s3_client():
    """Boto3 S3 클라이언트를 생성합니다."""
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    else: # IAM 역할 등을 통해 자격 증명 자동 감지할 경우 액세스 키 필요 없음
        return boto3.client('s3', region_name=AWS_REGION)


def load_model_from_s3():
    """S3에서 모델을 다운로드하고 메모리에 로드합니다."""
    global _model

    if _model is not None:
        print("⭐️ [모델] 이미 메모리에 로드되어 있습니다.")
        return _model

    if not S3_MODEL_BUCKET or not S3_MODEL_KEY:
        print("❌ [모델] S3 버킷명 또는 모델 키가 설정되지 않았습니다.")
        return None

    s3 = get_s3_client()
    try:
        print(f"💡 [모델] s3://{S3_MODEL_BUCKET}/{S3_MODEL_KEY} 에서 모델 다운로드 중...")
        obj = s3.get_object(Bucket=S3_MODEL_BUCKET, Key=S3_MODEL_KEY)
        model_str = obj['Body'].read().decode('utf-8')
        booster = lgb.Booster(model_str=model_str)
        # 래퍼로 LGBMRegressor를 만들어주는 방법
        _model = booster
        print("✅ [모델] 모델을 성공적으로 로드했습니다.")
        return _model
    except Exception as e:
        print(f"🚨 [모델] S3에서 모델을 불러오는 중 오류 발생: {e}")
        _model = None
        return None

def get_model():
    """로드된 모델 인스턴스를 반환합니다. 로드되지 않았다면 로드를 시도합니다."""
    global _model
    if _model is None:
        print("🔄 [모델] 모델이 메모리에 없어 S3에서 로드 시도합니다...")
        load_model_from_s3()
    return _model


def predict(df_wide):
    """
    전처리된 DataFrame(df_wide)을 받아 예측 결과 반환.
    실패 시 None 반환.
    """
    model = get_model()

    if model is None:
        print("❌ [예측] 모델이 로드되지 않아 예측할 수 없습니다.")
        return None

    if df_wide is None or df_wide.empty:
        print("❌ [예측] 입력 데이터가 비어있거나 로드 실패.")
        return None

    # 모델 입력 컬럼 (학습 피처와 반드시 일치)
    input_cols = [
        'temperature', 'pressure', 'vibration', 'humidity',
        'active_power', 'reactive_power',
        'active_power_rollmean', 'active_power_rollstd',
        'reactive_power_rollmean', 'reactive_power_rollstd',
        'power_factor',
        'temperature_rollmean', 'temperature_rollstd',
        'pressure_rollmean', 'pressure_rollstd',
        'vibration_rollmean', 'vibration_rollstd',
        'humidity_rollmean', 'humidity_rollstd',
        'equipment'
    ]

    # equipment 컬럼 category 변환
    if 'equipment' in df_wide.columns:
        df_wide['equipment'] = df_wide['equipment'].astype('category')

    # 숫자형만 fillna(0)
    num_cols = [c for c in input_cols if c != 'equipment']
    X = df_wide[num_cols].fillna(0)
    X['equipment'] = df_wide['equipment']

    print(f"✅ [예측] 모델 입력 shape: {X.shape}")

    # 예측
    try:
        y_pred = model.predict(X)
        print(f"✅ [예측] 예측 성공! 결과: {y_pred.tolist()}")
        return y_pred.tolist()
    except Exception as e:
        print(f"🚨 [예측] 예측 과정에서 오류 발생: {e}")
        return None
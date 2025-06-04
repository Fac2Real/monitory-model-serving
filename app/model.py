import boto3
import joblib
import os
from dotenv import load_dotenv
from io import BytesIO

# (input_data.py의 전처리 함수/데이터 불러오기 함수 import)
from app.input_data import preprocess_input_data, load_input_data_from_s3

# 환경변수 로드
load_dotenv()

AWS_REGION = os.getenv('AWS_REGION')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_MODEL_BUCKET = os.getenv('S3_MODEL_BUCKET_NAME', 'monitory-model')
S3_MODEL_KEY = os.getenv('S3_MODEL_KEY', 'models/lgbm_regressor.pkl')

_model = None

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
        print("⭐️Model already loaded in memory.")
        return _model

    if not S3_MODEL_BUCKET or not S3_MODEL_KEY:
        print("❌ Error: S3_MODEL_BUCKET_NAME or S3_MODEL_KEY is not set.")
        return None

    s3 = get_s3_client()

    # try:
    #     print(f"✅Downloading model from s3://{S3_MODEL_BUCKET}/{S3_MODEL_KEY} ")
    #

    obj = s3.get_object(Bucket=S3_MODEL_BUCKET, Key=S3_MODEL_KEY)
    model = joblib.load(BytesIO(obj['Body'].read()))
    print(f"✅ 모델을 s3://{S3_MODEL_BUCKET}/{S3_MODEL_KEY}에서 로드했습니다.")
    return model


#     s3_client = get_s3_client()
#     try:
#         print(f"Downloading model from s3://{S3_MODEL_BUCKET_NAME}/{S3_MODEL_KEY} to {LOCAL_MODEL_PATH}")
#         os.makedirs(os.path.dirname(LOCAL_MODEL_PATH), exist_ok=True)
#         s3_client.download_file(S3_MODEL_BUCKET_NAME, S3_MODEL_KEY, LOCAL_MODEL_PATH)

#         print(f"Loading model from {LOCAL_MODEL_PATH}")
#         _model = joblib.load(LOCAL_MODEL_PATH)
#         print("Model loaded successfully.")
#         return _model
#     except Exception as e:
#         print(f"Error loading model from S3: {e}")
#         _model = None # 로드 실패 시 None으로 설정
#         return None

# def get_model():
#     """로드된 모델 인스턴스를 반환합니다. 로드되지 않았다면 로드를 시도합니다."""
#     global _model
#     if _model is None:
#         print("Model not loaded. Attempting to load from S3...")
#         load_model_from_s3()
#     return _model

# def predict_from_s3_data(
#     s3_bucket: Optional[str] = Query(None, description="S3 bucket name for input data. Uses .env default if not provided."),
#     s3_key: Optional[str] = Query(None, description="S3 key (path) for input data CSV file. Uses .env default if not provided.")
# ):
#     """
#     S3에서 지정된 (또는 .env에 설정된 기본) CSV 데이터를 불러와 예측을 수행합니다.
#     """
#     # 1. 모델 로드
#     model = model_loader.get_model()
#     if model is None:
#         raise HTTPException(status_code=503, detail="Model not loaded. Cannot make predictions.")

#     # 2. S3에서 입력 데이터 로드
#     input_df = input_loader.load_input_data_from_s3(bucket_name=s3_bucket, data_key=s3_key)
#     if input_df is None:
#         raise HTTPException(status_code=404, detail=f"Could not load input data from S3 path: s3://{s3_bucket or input_loader.S3_INPUT_DATA_BUCKET_NAME_DEFAULT}/{s3_key or input_loader.S3_INPUT_DATA_KEY_DEFAULT}")

#     # 3. 데이터 전처리 (input_loader 또는 여기서 직접)
#     # 이 예시에서는 DataFrame을 직접 모델에 전달 가능한 형태로 변환한다고 가정
#     # 실제로는 input_loader.preprocess_input_data(input_df) 같은 함수 호출
#     try:
#         # Scikit-learn 모델은 보통 DataFrame이나 NumPy 배열을 입력으로 받음
#         # 모델이 학습될 때 사용된 피처 이름과 순서가 일치해야 함
#         # 예시: DataFrame에서 특정 컬럼만 선택하여 모델 입력으로 사용
#         # features_for_prediction = input_df[['col1', 'col2', 'col3']] # 실제 컬럼명으로 변경
#         # 혹은 input_df.values (NumPy 배열로 변환)
#         if input_df.empty:
#              raise ValueError("Input data DataFrame is empty after loading from S3.")
        
#         # 이 부분은 모델이 어떤 입력을 기대하느냐에 따라 크게 달라집니다.
#         # 가장 간단하게는 DataFrame의 모든 숫자형 데이터를 사용하거나,
#         # 학습 시 사용한 피처만 선택해야 합니다.
#         # 여기서는 DataFrame을 직접 predict에 넣는다고 가정 (모델이 이를 처리할 수 있어야 함)
#         print(f"Making predictions on data with shape: {input_df.shape}")
#         predictions_raw = model.predict(input_df) # 모델에 따라 입력 형태 조정 필요

#         # 4. 결과 후처리
#         if hasattr(predictions_raw, 'tolist'):
#             predictions_list = predictions_raw.tolist()
#         else:
#             predictions_list = list(predictions_raw) # 혹은 다른 변환

#         data_source_path = f"s3://{s3_bucket or input_loader.S3_INPUT_DATA_BUCKET_NAME_DEFAULT}/{s3_key or input_loader.S3_INPUT_DATA_KEY_DEFAULT}"
#         return schemas.PredictionResult(input_data_source=data_source_path, predictions=predictions_list)

#     except Exception as e:
#         print(f"Error during prediction process: {e}")
#         # 스택 트레이스를 포함하여 로깅하는 것이 디버깅에 더 좋습니다.
#         # import traceback
#         # traceback.print_exc()
#         raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

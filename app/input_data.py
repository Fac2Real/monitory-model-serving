import boto3
import pandas as pd
import io
import os
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

load_dotenv()

S3_INPUT_DATA_BUCKET_NAME = os.getenv("S3_INPUT_DATA_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def get_s3_client_for_input():
    """Boto3 S3 클라이언트를 생성합니다."""
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    else:
        return boto3.client('s3', region_name=AWS_REGION)

def get_s3_key_for_input(zoneId, equipId):
    """equipId와 zoneId를 바탕으로 S3 디렉토리를 지정합니다."""
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    one_hour_ago = now - timedelta(hours=1)
    date = one_hour_ago.strftime("%Y-%m-%d")

    s3_key = f"EQUIPMENT/date={date}/zone_id={zoneId}/equip_id={equipId}/"

    # 로그 출력
    print(f"✅ S3 Key 생성 정보 - date: {date}, zoneId: {zoneId}, equipId: {equipId}")
    return s3_key

"""
S3에서 최신 1시간 데이터 불러오기
"""
def load_input_data_from_s3(zoneId, equipId): 
    target_bucket = S3_INPUT_DATA_BUCKET_NAME
    target_key = get_s3_key_for_input(zoneId, equipId)

    if not target_bucket or not target_key:
        print("❌ Error: S3 input data bucket name or key is not set.")
        return None

    s3_client = get_s3_client_for_input()
    latest_file_key = None
    latest_mod_time = None

    try:
        print(f"💡 s3://{target_bucket}/{target_key} 경로의 객체를 나열합니다.")
        # 해당 디렉토리(접두사)의 객체 목록 가져오기
        # list_objects_v2는 페이징 처리가 필요할 수 있지만, 여기서는 간단히 첫 페이지 가정
        response = s3_client.list_objects_v2(Bucket=target_bucket, Prefix=target_key)

        if 'Contents' not in response:
            print(f"❌ S3에 없는 파일 경로://{target_bucket}/{target_key}")
            return None

        # 파일 확장자에 맞는 파일들만 필터링하고 최신 파일 찾기
        for obj in response['Contents']:
            key = obj['Key']
            # 디렉토리 자체(키가 접두사와 같고 /로 끝나는 경우)이거나, 원하는 확장자가 아니면 건너뛰기
            if key == target_key or not key.endswith(".json"):
                continue

            mod_time = obj['LastModified']
            if latest_mod_time is None or mod_time > latest_mod_time:
                latest_mod_time = mod_time
                latest_file_key = key
        
        if latest_file_key is None:
            print(f"❌ s3://{target_bucket}/{target_key} 경로에 '.json' 확장자를 가진 파일이 없습니다.")
            return None

        print(f"⭐️ 최신 파일 발견: s3://{target_bucket}/{latest_file_key} (최종 수정일: {latest_mod_time})")

        # 최신 파일 내용 읽기
        file_response = s3_client.get_object(Bucket=target_bucket, Key=latest_file_key)
        file_content_bytes = file_response['Body'].read()
        file_content_string = file_content_bytes.decode('utf-8')

        # 파일 내용을 눈으로 확인하기 위해 DataFrame으로 변환 (또는 문자열 그대로 반환)
        # 여기서는 JSON Lines 형식이라고 가정하고 DataFrame으로 변환
        file_extension = ".jsonl"
        if file_extension.lower() in [".jsonl", ".ndjson"]:
            lines = [line for line in file_content_string.splitlines() if line.strip()]
            if not lines:
                print(f"🚨경고: 최신 파일 s3://{target_bucket}/{latest_file_key} 이(가) 비어있거나 공백만 포함하고 있습니다.")
                return pd.DataFrame() # 빈 DataFrame 반환
            df = pd.read_json(io.StringIO('\n'.join(lines)), lines=True)
        elif file_extension.lower() == ".json":
            # 일반 JSON 파일 처리 (구조에 따라 pd.read_json 또는 json.loads + pd.DataFrame/json_normalize)
            # 여기서는 간단히 pd.read_json(orient='records')를 가정
            # 실제 JSON 구조에 맞게 수정 필요
            df = pd.read_json(io.StringIO(file_content_string), orient='records')
        else:
            print(f"🚨지원하지 않는 파일 확장자(.json)입니다. 원본 내용을 반환합니다.")
            # DataFrame으로 변환하지 않고 원시 문자열 내용 반환 (또는 에러 처리)
            # API 응답 시 이 경우를 고려해야 함
            return {"file_key": latest_file_key, "raw_content": file_content_string}


        print(f"⭐️ 최신 파일의 데이터를 성공적으로 불러왔습니다. 데이터 형태: {df.shape}")
        if df.empty:
            print(f"🚨경고: s3://{target_bucket}/{latest_file_key} 에서 불러온 DataFrame이 비어있습니다.")
        else:
            # 데이터 일부 샘플 출력 (최대 5행)
            print("\n----------------------------")
            print("\n 👀 데이터 프레임 미리보기 (최대 5행):")
            print(df.head())
            print("\n----------------------------")
        
        # 눈으로 확인하기 위해 DataFrame을 반환하거나,
        # API 응답에서 처리하기 쉽도록 to_dict('records') 등으로 변환하여 반환할 수 있습니다.
        return preprocess_input_data(df,5)
        # return df

    except s3_client.exceptions.NoSuchKey:
        # 이 예외는 get_object 호출 시 발생할 수 있으나, list_objects_v2로 먼저 확인하므로 발생 빈도 낮음
        print(f"Error: Specific file not found during get_object (should not happen if list_objects was successful).")
        return None
    except Exception as e:
        print(f"🚨 S3 디렉토리 s3://{target_bucket}/{target_key} 에서 최신 입력 데이터를 불러오는 중 오류가 발생했습니다: {e}")
        import traceback
        traceback.print_exc()
        return None

"""
데이터 전처리 함수
"""
def preprocess_input_data(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    """
    S3 등에서 로드한 DataFrame을 모델 입력용 wide 형태로 전처리합니다.
    - 시간순 정렬
    - rolling mean/std 파생 변수 생성
    - sensorType 필터링 및 한글 컬럼명 매핑
    - 그룹 집계 및 wide pivot 변환
    - power_factor 생성
    """
    if df is None or df.empty:
        print("❌ 입력 데이터가 없습니다.")
        return None

    print("📊 [1] 시간순 정렬 중...")
    df = df.sort_values(['equipId', 'sensorType', 'time'])
    print(df.head())

    print("\n📊 [2] rolling mean/std 계산 중...")
    df['val_rollmean'] = (
        df.groupby(['equipId', 'sensorType'])['val']
        .rolling(window=window, min_periods=1)
        .mean()
        .reset_index(level=[0,1], drop=True)
    )
    df['val_rollstd'] = (
        df.groupby(['equipId', 'sensorType'])['val']
        .rolling(window=window, min_periods=1)
        .std()
        .reset_index(level=[0,1], drop=True)
    )
    print(df[['equipId', 'sensorType', 'val', 'val_rollmean', 'val_rollstd']].head())

    print("\n📊 [3] sensorType 매핑 및 필터링 중...")
    mapping = {
        'temp': 'temperature',
        'humid': 'humidity',
        'pressure': 'pressure',
        'vibration': 'vibration',
        'reactive_power': 'reactive_power',
        'active_power': 'active_power',
        # 필요시 다른 sensorType도 추가
    }
    df = df[df['sensorType'].isin(mapping.keys())]

    print("\n📊 [4] 그룹 집계(mean) 중...")
    agg_df = (
        df.groupby(['equipId', 'sensorType'])[['val', 'val_rollmean', 'val_rollstd']]
        .mean()
        .reset_index()
    )
    print(agg_df.head())

    print("\n📊 [5] wide 형태로 pivot 변환 중...")
    pivot_cols = ['val', 'val_rollmean', 'val_rollstd']
    df_wide = agg_df.pivot(
        index=['equipId'],
        columns='sensorType',
        values=pivot_cols
    ).reset_index()
    print(df_wide.head())

    print("\n📊 [6] 컬럼명 평탄화(flatten) 중...")
    df_wide.columns = [
        col[0] if col[0] == 'equipId'
        else (
            f"{mapping.get(col[1], col[1])}" if col[0] == 'val'
            else f"{mapping.get(col[1], col[1])}_{col[0].replace('val_', '')}"
        )
        for col in df_wide.columns
    ]
    df_wide = df_wide.rename(columns={'equipId': 'equipment'})
    print(df_wide.head())

    print("\n📊 [7] power_factor 생성 중...")
    if 'active_power' in df_wide.columns and 'reactive_power' in df_wide.columns:
        df_wide['power_factor'] = (
            df_wide['active_power'] /
            (df_wide['active_power']**2 + df_wide['reactive_power']**2)**0.5
        )
        print("power_factor 생성 완료")
    else:
        print("active_power, reactive_power 컬럼이 없어 power_factor 생성 생략")

    print("\n✅ 전처리 완료! 최종 데이터 샘플:")
    print(df_wide.head())

    return df_wide


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
    return s3_key


def load_input_data_from_s3(zoneId, equipId): 
    target_bucket = S3_INPUT_DATA_BUCKET_NAME
    target_key = get_s3_key_for_input(zoneId, equipId)

    if not target_bucket or not target_key:
        print("Error: S3 input data bucket name or key is not set.")
        return None

    s3_client = get_s3_client_for_input()
    latest_file_key = None
    latest_mod_time = None

    try:
        print(f"Listing objects in s3://{target_bucket}/{target_key}")
        # 해당 디렉토리(접두사)의 객체 목록 가져오기
        # list_objects_v2는 페이징 처리가 필요할 수 있지만, 여기서는 간단히 첫 페이지 가정
        response = s3_client.list_objects_v2(Bucket=target_bucket, Prefix=target_key)

        if 'Contents' not in response:
            print(f"No files found in s3://{target_bucket}/{target_key}")
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
            print(f"No files with extension '.json' found in s3://{target_bucket}/{target_key}")
            return None

        print(f"Latest file found: s3://{target_bucket}/{latest_file_key} (LastModified: {latest_mod_time})")

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
                print(f"Warning: Latest file s3://{target_bucket}/{latest_file_key} is empty or contains only whitespace.")
                return pd.DataFrame() # 빈 DataFrame 반환
            df = pd.read_json(io.StringIO('\n'.join(lines)), lines=True)
        elif file_extension.lower() == ".json":
            # 일반 JSON 파일 처리 (구조에 따라 pd.read_json 또는 json.loads + pd.DataFrame/json_normalize)
            # 여기서는 간단히 pd.read_json(orient='records')를 가정
            # 실제 JSON 구조에 맞게 수정 필요
            df = pd.read_json(io.StringIO(file_content_string), orient='records')
        elif file_extension.lower() == ".csv":
            df = pd.read_csv(io.StringIO(file_content_string))
        else:
            print(f"Unsupported file extension for parsing: .json. Returning raw content.")
            # DataFrame으로 변환하지 않고 원시 문자열 내용 반환 (또는 에러 처리)
            # API 응답 시 이 경우를 고려해야 함
            return {"file_key": latest_file_key, "raw_content": file_content_string}


        print(f"Data from latest file loaded successfully. Shape: {df.shape}")
        if df.empty:
            print(f"Warning: Loaded DataFrame from s3://{target_bucket}/{latest_file_key} is empty.")
        
        # 눈으로 확인하기 위해 DataFrame을 반환하거나,
        # API 응답에서 처리하기 쉽도록 to_dict('records') 등으로 변환하여 반환할 수 있습니다.
        return df 

    except s3_client.exceptions.NoSuchKey:
        # 이 예외는 get_object 호출 시 발생할 수 있으나, list_objects_v2로 먼저 확인하므로 발생 빈도 낮음
        print(f"Error: Specific file not found during get_object (should not happen if list_objects was successful).")
        return None
    except Exception as e:
        print(f"Error loading latest input data from S3 directory s3://{target_bucket}/{target_key}: {e}")
        import traceback
        traceback.print_exc()
        return None

# (선택 사항) 데이터 전처리 함수
def preprocess_input_data(df: pd.DataFrame):
    """
    로드된 DataFrame을 모델 입력 형식에 맞게 전처리합니다.
    이 부분은 모델 학습 시 사용한 전처리 방식과 일치해야 합니다.
    """
    if df is None:
        return None
    print("Preprocessing input data...")
    # 예시: 특정 컬럼만 선택하거나, 스케일링 등을 수행
    # features = df[['feature1', 'feature2', 'feature3']].values.tolist()
    # 실제 모델의 입력 형태에 맞게 수정 (예: NumPy 배열, 리스트의 리스트 등)
    # 이 예제에서는 DataFrame 전체를 반환하고, main.py에서 필요한 부분만 사용한다고 가정
    return df


# --- 여기부터 데이터를 열어보는 코드 ---
if __name__ == "__main__":
    example_zone_id = "20250507165750-827"  # 실제 zone_id로 변경
    example_equip_id = "20250507171316-389" # 실제 equip_id로 변경

    print(f"Attempting to load data for zone_id='{example_zone_id}', equip_id='{example_equip_id}'...")
    
    # S3_INPUT_DATA_BUCKET_NAME이 설정되었는지 확인
    if not S3_INPUT_DATA_BUCKET_NAME:
        print("환경변수 S3_INPUT_DATA_BUCKET_NAME이 설정되지 않았습니다.")
        print("'.env' 파일에 S3_INPUT_DATA_BUCKET_NAME='your-bucket-name' 형식으로 추가하거나 직접 설정해주세요.")
    else:
        loaded_dataframe = load_input_data_from_s3(example_zone_id, example_equip_id)

        if loaded_dataframe is not None:
            if isinstance(loaded_dataframe, pd.DataFrame):
                print("\n--- 로드된 DataFrame의 내용 (처음 5줄) ---")
                print(loaded_dataframe.head())

                print("\n--- DataFrame 정보 ---")
                loaded_dataframe.info()

                # DataFrame의 모든 내용을 보고 싶다면 (데이터가 매우 크면 터미널에 모두 표시하기 어려울 수 있음)
                print("\n--- 전체 DataFrame 내용 ---")
                pd.set_option('display.max_rows', None) # 모든 행 표시
                pd.set_option('display.max_columns', None) # 모든 열 표시
                pd.set_option('display.width', None) # 너비 제한 없음
                print(loaded_dataframe)
            elif isinstance(loaded_dataframe, dict) and "raw_content" in loaded_dataframe:
                print("\n--- DataFrame으로 파싱 실패, 원본 내용 ---")
                print(f"File Key: {loaded_dataframe.get('file_key')}")
                print("Raw Content:")
                print(loaded_dataframe.get('raw_content')[:500] + "..." if len(loaded_dataframe.get('raw_content', '')) > 500 else loaded_dataframe.get('raw_content'))
                if "error" in loaded_dataframe:
                    print(f"Parsing Error: {loaded_dataframe.get('error')}")
        else:
            print(f"\n데이터 로드에 실패했거나, {example_zone_id}/{example_equip_id} 경로에 해당 파일이 없습니다.")
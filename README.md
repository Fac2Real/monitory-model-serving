# Monitory ML Service

**스마트 팩토리 설비 센서 데이터를 기반으로 잔존 수명(RUL) 예측 및 모델 재학습 파이프라인**을 FastAPI 서버로 제공하는 프로젝트입니다.

---


## 📁 프로젝트 구조

```plaintext
monitory-model-server/
├── app/
│   ├── api/                   # FastAPI 라우터 (predict, health)
│   ├── core/                  # 설정(config), 상수(constants), 로깅 설정
│   ├── service/               # 비즈니스 로직 (data_service, model_service, retrain_service)
│   ├── scheduler.py           # APScheduler 기반 일일 재학습 잡
│   └── main.py                # FastAPI 애플리케이션 엔트리포인트
├── tasks/                     # 수동 CLI용 retrain 스크립트
│   └── retrain.py
├── requirements.txt           # Python 패키지 의존성
├── .env.example               # 환경변수 템플릿
└── README.md                  # 프로젝트 안내서 (이 파일)
```

---

## 🔄 작업 흐름

1. **추론 요청** (`/api/v1/predict`):

   - `data_service` → S3에서 최신 1시간치 센서 JSON 로드 → `preprocess_input_data`로 wide 포맷 생성
   - `model_service` → S3에서 latest 모델 로드(ETag 캐시 적용) → 예측 결과 반환

     

2. **수동 재학습** (`tasks/retrain.py`):

   ```bash
   export PYTHONPATH=$(pwd)
   python -m tasks.retrain --month YYYY-MM [--sample N]
   ```

   - 지정 월 데이터 S3에서 로드 → 전처리 → Balancing → LightGBM 재학습 → S3에 버전 저장 및 최신 모델 승격

     

3. **일일 자동 재학습** (`app/scheduler.py` + APScheduler):

   - 매일 자정(KST) `run_retrain_job()` 실행
     - 최근 21일치 데이터 sufficiency 체크 → 기준 미충족 시 Skip
     - `retrain_service.train_and_upload()` 호출 
    
       

---

## ⚙️ 환경 설정

1. `.env` 파일 생성 (루트에 복사) 및 변수 설정:

   ```ini
   AWS_REGION=ap-northeast-2
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...

   S3_MODEL_BUCKET_NAME=monitory-model
   S3_MODEL_KEY=models/latest/lgbm_regressor.json

   S3_INPUT_DATA_BUCKET_NAME=monitory-bucket
   S3_INPUT_DATA_KEY=EQUIPMENT/

   LOG_LEVEL=INFO
   LOG_FORMAT=TEXT
   LOG_EMOJI=true
   ```


2. 가상환경 및 의존성 설치:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

---

## ▶️ 서버 실행

```bash
export PYTHONPATH=$(pwd)
uvicorn app.main:app
```


- FastAPI 애플리케이션과 APScheduler 스케줄러가 함께 실행됩니다.
- `/api/v1/predict?zoneId=<zone>&equipId=<equip>` 호출로 예측 사용 가능

---


## ▶️ 수동 재학습 테스트

```bash
export PYTHONPATH=$(pwd)
python -m tasks.retrain --month 2025-06 --sample 500
```


---

## 📦 주요 파일

- `app/service/retrain_service.py`: 재학습·버전관리·S3 업로드 로직
- `tasks/retrain.py`: 수동 CLI 인터페이스
- `app/scheduler.py`: 일일 자동 재학습 잡 및 데이터 sufficiency 체크

---


## 🛠️ 기타

- **로깅**: `app/core/logging_config.py` (emoji 옵션 지원)
- **상수**: `app/core/constants.py`
- **설정**: `app/core/config.py` (Pydantic BaseSettings)

---

**문의 및 기여 환영합니다!**


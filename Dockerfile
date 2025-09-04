# 1단계: 베이스 컴퓨터 선택 (이전과 동일)
FROM python:3.9-slim-bookworm

# ⭐ [추가됨] 2단계: 필수 프로그램 설치 (가스레인지 설치)
# apt-get은 Mac의 brew와 같은 리눅스용 프로그램 설치 관리자입니다.
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# ⭐ [추가됨] 3단계: 자바 경로 설정 (가스레인지 위치 알려주기)
# 가상 컴퓨터에게 자바가 어디에 설치되었는지 알려줍니다.
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

# 4단계: 작업 공간 만들기 (이전과 동일)
WORKDIR /app

# 5단계: 파이썬 라이브러리 설치 (이전과 동일)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6단계: 내 프로젝트 파일 복사하기 (이전과 동일)
COPY . .

# 7단계: 자동 실행 명령어 지정 (이전과 동일)
CMD ["python3", "pipeline.py"]
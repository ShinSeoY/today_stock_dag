FROM apache/airflow:2.9.1

USER root

# Playwright에 필요한 시스템 의존성 설치
RUN apt-get update && apt-get install -y \
    # 기본 도구들
    wget curl gnupg ca-certificates \
    # Playwright/Chromium 의존성들
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxcomposite1 \
    libxrandr2 libxdamage1 libxss1 libasound2 libxshmfence1 libgbm1 \
    libx11-xcb1 libxfixes3 libxkbcommon0 libgtk-3-0 libgdk-pixbuf2.0-0 \
    libxcomposite1 libxcursor1 libxi6 libxtst6 libasound2-dev \
    && rm -rf /var/lib/apt/lists/*

# 파일 복사
COPY ./requirements.txt /opt/airflow/requirements.txt

# airflow 사용자로 전환
USER airflow

# requirements 설치
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# playwright 설치 및 브라우저 설치
RUN pip install --no-cache-dir "playwright==1.*" && \
    playwright install chromium
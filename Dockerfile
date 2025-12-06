FROM python:3.12-slim

WORKDIR /app

# نصب ابزارهای سیستمی لازم برای build کردن پکیج‌ها مثل tortoise-orm, asyncpg
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY ../requirements.txt .

# نصب پکیج‌ها (با آپدیت pip)
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# کپی کل پروژه
COPY . .

CMD ["python", "-m", "crawler.main"]
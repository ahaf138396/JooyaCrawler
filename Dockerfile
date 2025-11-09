FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# کپی کل پوشه‌ی پروژه (که شامل crawler است)
COPY . .

# کار در مسیر اصلی پروژه
WORKDIR /app/crawler

CMD ["python", "-m", "crawler.main"]

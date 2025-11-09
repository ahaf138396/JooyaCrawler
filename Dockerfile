FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# اطمینان از اینکه ساختار درست کپی میشه
COPY crawler /app/crawler

WORKDIR /app/crawler

CMD ["python", "-m", "crawler.main"]

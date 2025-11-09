FROM python:3.12-slim

WORKDIR /app

COPY ../requirements.txt .

RUN pip uninstall -y redis redis-py aioredis || true && \
    pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "crawler.main"]

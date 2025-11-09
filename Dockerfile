FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# تمام پوشه پروژه را کپی می‌کنیم
COPY . .

# ورود به مسیر پروژه که ماژول crawler آنجاست
WORKDIR /app/crawler

CMD ["python", "-m", "main"]

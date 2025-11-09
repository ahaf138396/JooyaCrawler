FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# کل پروژه را کپی کن
COPY . .

# تنظیم PYTHONPATH تا مسیرها از /app قابل import باشند
ENV PYTHONPATH=/app

# اجرای ماژول crawler.main به صورت مطلق
CMD ["python", "-m", "crawler.main"]


---

# 📌 JooyaCrawler — High-Performance Asynchronous Web Crawler

**Jooya Search Engine – MVP Core Crawler Module**
Built with **Python 3.12**, **AsyncIO**, **PostgreSQL**, **MongoDB**, **Tortoise ORM**, **HTTPX**, and fully containerized with **Docker Compose**

---

## 📖 Overview

JooyaCrawler هسته اصلی سیستم خزش در MVP موتور جست‌وجوی جویا است.
این سرویس به صورت **کاملاً Async و Event-Driven** طراحی شده و شامل اجزاء زیر است:

* **Queue Management (PostgreSQL)**
* **Raw HTML Storage (MongoDB)**
* **Parallel Workers**
* **Periodic Scheduler**
* **HTML Parsing Engine (BeautifulSoup + Custom Logic)**
* **ORM Schema Auto-Initialization**
* **Dockerized Architecture**
* **High-scalability, fault-tolerant pipeline**

این ماژول پایه اصلی خزش، ایندکس اولیه و هدف نهایی:
🔥 ایجاد کامل‌ترین موتور جست‌وجوی فارسی بدون وابستگی به سرویس خارجی.

---

# 🚀 Features

### ✅ **Asynchronous Architecture**

ساخته شده با `asyncio`، سازگار با هزاران اتصال همزمان.

### ✅ **PostgreSQL-Based Queue System**

مدیریت صف URLs با:

* جلوگیری از دوباره‌کاری
* Atomic operations
* حالت‌های pending / processing / done

### ✅ **MongoDB Storage**

ذخیره‌ی کامل HTML، headers، status code و متادیتا.

### ✅ **HTML Parsing Engine**

* استخراج عنوان
* استخراج لینک‌های داخلی
* نرمال‌سازی لینک‌ها
* جلوگیری از کراول external domains
* حذف لینک‌های تکراری

### ✅ **Parallel Workers**

سه Worker پیش‌فرض (قابل افزایش):

* Download
* Save
* Extract Links
* Enqueue new tasks

### ✅ **Scheduler**

افزودن دوره‌ای لینک‌ها، heartbeat، آمار و seed-refresh.

### ✅ **Auto Schema Initialization**

در صورت عدم وجود جدول‌ها → خودکار ساخته می‌شوند.

### ✅ **Dockerized Setup**

اجرای کامل با یک دستور:

```
docker compose up -d
```

---

# 📂 Project Structure

```
JooyaCrawler/
├── crawler/
│   ├── main.py
│   ├── worker.py
│   ├── parsing/
│   │   └── html_extractor.py
│   ├── storage/
│   │   ├── models/
│   │   │   ├── page_model.py
│   │   │   ├── queue_model.py
│   │   ├── mongo/
│   │   │   ├── mongo_manager.py
│   │   │   └── mongo_storage_manager.py
│   │   └── postgres/
│   │       ├── postgres_init.py
│   │       ├── postgres_manager.py
│   │       └── postgres_queue_manager.py
│   └── monitoring/
│       └── storage_monitor.py
│
├── Dockerfile
├── requirements.txt
└── README.md
```

---

# 🛠 Installation (Development)

### 1. Clone

```
git clone https://github.com/.../JooyaCrawler.git
cd JooyaCrawler
```

### 2. Install Dependencies

```
pip install -r requirements.txt
```

### 3. Configure Environment

متغیرهای محیطی مهم در فایل `.env` ریشه پروژه تعریف شده‌اند. مقادیر پیش‌فرض شامل:

```
POSTGRES_USER=jooya
POSTGRES_PASSWORD=postgres
POSTGRES_DB=jooyacrawlerdb
DATABASE_URL=postgresql://jooya:postgres@postgres:5432/jooyacrawlerdb
MONGO_URI=mongodb://localhost:27017/jooyacrawlerdb
REDIS_URL=redis://localhost:6379/0
WORKERS=12
```

در صورت نیاز به تغییر، فایل `.env` را ویرایش کنید یا متغیرها را قبل از اجرا ست نمایید.

---

# 🐳 Run With Docker (Recommended)

```
docker compose up -d
```

### Check logs:

```
docker compose logs -f crawler
```

---

# ⚙️ Architecture Details

## Queue Manager (PostgreSQL)

* ذخیره URL
* جلوگیری از تکرار
* atomic dequeue
* وضعیت‌ها: pending / processing / done

## Worker Engine

* گرفتن URL از Queue
* دانلود Async
* ذخیره در Mongo
* استخراج لینک
* اضافه کردن لینک‌ها به صف
* علامت‌گذاری انجام‌شده

## Scheduler

* Seed URL
* تزریق دوره‌ای
* کنترل سرعت و حجم دیتابیس
* بک‌آف هوشمند

## Mongo Layer

* آرشیو HTML
* امکان ذخیره نسخه‌های مختلف

---

# 🧪 Example Log Output

```
[Worker-1] Crawled: https://example.com/page/23 (6342 bytes)
[Worker-1] Found and queued 182 links from https://example.com/page/23
Scheduler: Added https://example.com/page/24
PostgreSQL: Verified 2 tables
```

---

# 🔒 Storage Monitoring (Coming Soon)

* هشدار درصورت افزایش اندازه دیتابیس
* کاهش سرعت crawl → جلوگیری از پر شدن دیسک
* metrics برای Prometheus

---

# 📈 Future Improvements

* Distributed crawling cluster
* URL canonicalization
* Duplicate content detection
* Robots.txt engine
* Sitemap crawler
* Anti-loop protection
* Rate-limit smoothing
* Crawling policies per domain
* Web-based dashboard

---

# 🤝 Contributing

Pull Requests are welcome!

---


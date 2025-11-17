from tortoise import fields, models

class DomainCrawlPolicy(models.Model):
    id = fields.IntField(pk=True)

    domain = fields.CharField(max_length=255, unique=True)

    # فاصله زمانی بین هر درخواست (به میلی‌ثانیه)
    min_delay_ms = fields.IntField(default=1000)

    # آخرین زمان crawl برای کنترل نرخ
    last_crawled_at = fields.DatetimeField(null=True)

    # محدودیت تعداد صفحات در روز
    daily_limit = fields.IntField(default=10000)
    crawled_today = fields.IntField(default=0)

    class Meta:
        table = "domain_crawl_policy"
        indexes = ("domain",)

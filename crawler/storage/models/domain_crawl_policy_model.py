from tortoise import fields, models


class DomainCrawlPolicy(models.Model):
    """
    کنترل نرخ خزش هر دامنه (crawl politeness).
    """
    id = fields.IntField(pk=True)

    domain = fields.CharField(max_length=255, unique=True)

    # حداقل فاصله بین دو درخواست (میلی‌ثانیه)
    min_delay_ms = fields.IntField(default=1000)

    last_crawled_at = fields.DatetimeField(null=True)

    next_allowed_at = fields.DatetimeField(null=True)

    daily_limit = fields.IntField(default=10000)
    crawled_today = fields.IntField(default=0)

    class Meta:
        table = "domain_crawl_policy"
        indexes = ("domain",)

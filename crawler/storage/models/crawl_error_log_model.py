from tortoise import fields, models


class CrawlErrorLog(models.Model):
    """
    لاگ خطاهای خزش.
    """
    id = fields.IntField(pk=True)

    url = fields.CharField(max_length=2048, index=True)
    status_code = fields.IntField(null=True)
    error_message = fields.TextField(null=True)
    timestamp = fields.DatetimeField(auto_now_add=True)
    worker_id = fields.IntField(null=True)

    class Meta:
        table = "crawl_error_logs"
        indexes = ("url", "timestamp")

    async def save(self, *args, **kwargs):  # type: ignore[override]
        if self.error_message and len(self.error_message) > 512:
            self.error_message = self.error_message[:512]
        await super().save(*args, **kwargs)

from tortoise import fields, models


class CrawlQueue(models.Model):
    """
    صف URL ها برای خزش.
    """
    id = fields.IntField(pk=True)
    url = fields.CharField(max_length=2048, unique=True)
    status = fields.CharField(
        max_length=20,
        default="pending",
        index=True,  # pending / processing / done / error
    )
    priority = fields.IntField(default=0, index=True)
    scheduled_at = fields.DatetimeField(auto_now_add=True)
    last_attempt_at = fields.DatetimeField(null=True)
    error_count = fields.IntField(default=0)

    class Meta:
        table = "crawl_queue"
        indexes = ("status", "priority")

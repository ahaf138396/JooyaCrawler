from tortoise import fields, models

class CrawlQueue(models.Model):
    id = fields.IntField(pk=True)

    url = fields.CharField(max_length=2048, unique=True)

    status = fields.CharField(max_length=20, default="pending")  # pending, processing, done
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "crawl_queue"
        indexes = ("url",)

from tortoise import models, fields

class CrawlQueue(models.Model):
    id = fields.IntField(pk=True)
    url = fields.TextField(unique=True)   # جلوگیری از تکرار
    status = fields.CharField(max_length=20, default="pending")
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "crawl_queue"
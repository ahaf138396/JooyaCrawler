from tortoise import fields, models
from datetime import datetime

class CrawledPage(models.Model):
    id = fields.IntField(pk=True)
    url = fields.CharField(max_length=2048, unique=True)
    status_code = fields.IntField(null=True)
    title = fields.CharField(max_length=512, null=True)
    content = fields.TextField(null=True)
    crawl_time = fields.DatetimeField(default=datetime.utcnow)

    class Meta:
        table = "crawled_pages"

    def __str__(self):
        return f"{self.url} [{self.status_code}]"

from tortoise import models, fields


class CrawledPage(models.Model):
    id = fields.IntField(pk=True)
    url = fields.TextField(unique=True)

    status_code = fields.IntField()
    title = fields.TextField(null=True)
    content = fields.TextField(null=True)

    crawled_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "crawled_pages"

    def __str__(self):
        return f"{self.url} [{self.status_code}]"

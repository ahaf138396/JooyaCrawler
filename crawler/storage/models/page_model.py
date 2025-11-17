from tortoise import fields, models


class CrawledPage(models.Model):
    """
    اطلاعات پایه‌ای صفحات خزیده‌شده.
    """
    id = fields.IntField(pk=True)
    url = fields.CharField(max_length=2048, unique=True, index=True)
    status_code = fields.IntField()
    title = fields.CharField(max_length=512, null=True)
    content = fields.TextField(null=True)
    fetched_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "crawled_pages"
        indexes = ("url", "fetched_at")


    def __str__(self):
        return f"{self.url} [{self.status_code}]"





from tortoise import fields, models

class CrawlQueue(models.Model):
    id = fields.IntField(pk=True)
    url = fields.TextField()
    status = fields.CharField(max_length=20, default='pending')
    added_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "crawler_queue"

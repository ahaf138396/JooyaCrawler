from tortoise import fields, models


class OutboundLink(models.Model):
    """
    لینک‌های خروجی هر صفحه (برای گراف وب / PageRank و ...).
    """
    id = fields.IntField(pk=True)

    source_page = fields.ForeignKeyField(
        "models.CrawledPage",
        related_name="outbound_links",
        on_delete=fields.CASCADE,
    )

    target_url = fields.CharField(max_length=2048, index=True)
    is_internal = fields.BooleanField(default=True)
    discovered_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "outbound_links"
        indexes = ("target_url",)

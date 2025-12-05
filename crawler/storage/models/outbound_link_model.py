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

    @classmethod
    async def bulk_insert_links(cls, links: list[dict]):
        """در صورت نیاز لینک‌ها را به صورت batch درج می‌کند."""
        if not links:
            return

        instances = [cls(**link) for link in links]
        await cls.bulk_create(instances)

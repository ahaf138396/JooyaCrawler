from tortoise import fields, models


class PageMetadata(models.Model):
    """
    متادیتا و مشخصات تحلیلی صفحات.
    """
    id = fields.IntField(pk=True)

    page = fields.OneToOneField(
        "models.CrawledPage",
        related_name="metadata",
        on_delete=fields.CASCADE,
    )

    html_length = fields.IntField(null=True)
    text_length = fields.IntField(null=True)
    link_count = fields.IntField(null=True)
    language = fields.CharField(max_length=16, null=True, index=True)

    # برای پیدا کردن صفحات تکراری
    content_hash = fields.CharField(max_length=64, null=True, index=True)

    # مثلا لیست کلمات کلیدی اصلی صفحه
    keywords = fields.JSONField(null=True)

    class Meta:
        table = "page_metadata"

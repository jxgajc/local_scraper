import scrapy

class HybridCrawlerItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    status_code = scrapy.Field()
    request_type = scrapy.Field()
    source = scrapy.Field()
    meta_info = scrapy.Field()
    # 内部使用字段
    url_hash = scrapy.Field()
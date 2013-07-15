# Scrapy settings for scoala project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'scoala'

SPIDER_MODULES = ['scoala.spiders']
NEWSPIDER_MODULE = 'scoala.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'scoala (+http://www.yourdomain.com)'

DEFAULT_ITEM_CLASS = 'scoala.items.IpeenItem'
ITEM_PIPELINES = ['scoala.pipelines.MySQLStorePipeline']
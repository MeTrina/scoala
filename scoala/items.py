#-*-coding:utf-8-*-
# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class ScoalaItem(Item):
    # define the fields for your item here like:
    # name = Field()
    pass


class IpeenItem(Item):
    id = Field()
    name = Field()
    cate = Field()
    price = Field()
    score = Field()
    keywords = Field()
    image_url = Field()
    longitude = Field()
    latitude = Field()
    type = Field()
    phone_number = Field()
    address = Field()
    opening_hours = Field()
    off_day = Field()
    description = Field()
    url = Field()
    domain_id = Field()
    domain_url = Field()
    site_name = Field()
    page_url = Field()

    # traffic = Field()
    # seats = Field()
    # payment_type = Field()#only use for save tho single mongodb
    # accept_card_type = Field()#only use for save to shard mongodb
    # parking = Field()
    # media_info = Field()
    # media_recommend = Field()
    # business_info = Field()
    # update_time = Field()
    # recommendation = Field()
    # tag = Field()
    # average = Field()
    # location = Field()

    # def __str__(self):
    #     return 'IpeenItem(url: %s)' % self.url

class ShopProfileItem(Item):
    id = Field()
    name = Field()
    url = Field()
    domain_id = Field()
    domain_url = Field()


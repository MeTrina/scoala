from scrapy.selector import HtmlXPathSelector
from datetime import datetime
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.http import Request
from scoala.items import IpeenItem
from scoala.utils.select_result import list_first_item,strip_null,deduplication,clean_url
import re, sys
from scrapy.log import ScrapyFileLogObserver, logging
from scrapy import log
from hashlib import md5


class IpeenSpider(CrawlSpider):
    name = 'ipeen'
    allowed_domains = ['ipeen.com.tw']
    # start_urls = ['http://www.ipeen.com.tw/']
    start_urls = [
        'http://www.ipeen.com.tw/search/taipei/d6/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d7/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d8/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d9/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d10/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d11/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d12/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d13/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d14/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d15/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d16/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d17/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d18/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d19/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d20/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d21/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d22/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d23/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d24/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d25/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d26/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d27/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d28/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d29/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d30/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d31/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d32/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d33/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d34/6-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d35/6-100-0-0/'
    ]

    # rules = (
    #     Rule(SgmlLinkExtractor(allow=r'Items/'), callback='parse_item', follow=True),
    # )

    def __init__(self, name=None, **kwargs):
        LOG_ERROR = "scrapy__error_%s_%s.log" % (self.name, datetime.now().strftime("%Y-%m-%d-%H:%M:%S"))
        LOG_INFO = "scrapy__info_%s_%s.log" % (self.name, datetime.now().strftime("%Y-%m-%d-%H:%M:%S"))
        # ScrapyFileLogObserver
        ScrapyFileLogObserver(open(LOG_INFO, 'a'), level=logging.INFO).start()
        ScrapyFileLogObserver(open(LOG_ERROR, 'a'), level=logging.ERROR).start()
        # continue with the normal spider init
        super(IpeenSpider, self).__init__(name, **kwargs)

    def parse(self, response):
        hxs = HtmlXPathSelector(response)

        # --ok--
        shop_list_page = hxs.select('//*[@id="search"]/article/div[3]/div[1]/label[2]/a/@href').extract()
        if shop_list_page and shop_list_page[0]:
            page_account = int(shop_list_page[0].split("=")[-1])
            for page in xrange(1, page_account+1):
                # print  "p=%s" % page
                url = re.sub(r'p=\d*', 'p=%s' % page, shop_list_page[0])
                yield Request(url=url, callback=self.parse_profile)

    def parse_profile(self, response):
        hxs = HtmlXPathSelector(response)
        serMain = hxs.select('//*[@id="search"]/article') # main
        lbsResult = hxs.select('//section[contains(@class,"lbsResult")]/article') # lbsResult
        serShops = serMain.select('//div[contains(@class,"serShop")]/@id').extract() # shop_row_609926
        for shop in serShops:
            item = IpeenItem()
            shop_id = shop.split('_')[-1]
            item["id"] = shop_id
            item["domain_id"] = md5(response.url).hexdigest()
            item["domain_url"] = response.url
            url = 'http://www.ipeen.com.tw/shop/' + '%s' % str(shop_id)
            yield Request(url=url,
                          callback=self.parse_detail,
                          meta={'item': item}
            )

        # for one in lbsResult:
        #     shopProfile = IpeenItem()
        #     serShop = one.select('div[contains(@class,"serShop")]/@id').extract() # shop_row_609926
        #     # shopProfile["id"] = serShop[0].split('_')[-1]
        #     # shopProfile["name"] = one.select('div[contains(@class,"serShop")]/h3/a/text()').extract()[0]
        #     # shopProfile["url"] = 'http://www.ipeen.com.tw' + one.select('div[contains(@class,"serShop")]/h3/a/@href').extract()[0]
        #     shopProfile["domain_id"] = md5(response.url).hexdigest()
        #     shopProfile["domain_url"] = response.url
        #
        #     yield shopProfile

    def parse_detail(self, response):
        hxs = HtmlXPathSelector(response)
        item = response.meta['item']
        print "---------------------------------------------------------------------------------"
        # --ok--
        item["name"] = hxs.select('//meta[@property="og:title"]/@content').extract()[0]
        item["price"] = self.OnlyCharNum(hxs.select('concat(//*[@id="shop-metainfo"]/dl[2]/dd[1]/text(), "")').extract()[0].strip())
        item["cate"] = self.cleanItem(hxs.select('concat(//*[@id="shop-header"]/div[2]/div[2]/p[1]/a/text(), "")').extract())
        score = hxs.select('//*[@id="shop-header"]/div[2]/div[3]/p[1]/span/meter/@value').extract()
        item["score"] = int(score[0].strip()) if len(score) > 0 else 0
        item['keywords'] = hxs.select('//meta[@name="keywords"]/@content').extract()[0].encode('UTF-8')
        item["image_url"] = hxs.select('//meta[@property="og:image"]/@content').extract()[0]
        item["longitude"] = hxs.select('//meta[@property="place:location:longitude"]/@content').extract()[0]
        item["latitude"] = hxs.select('//meta[@property="place:location:latitude"]/@content').extract()[0]
        item["type"] = hxs.select('concat(//meta[@property="og:type"]/@content, "")').extract()[0].encode('UTF-8')
        item["phone_number"] = self.cleanItem(hxs.select('concat(//*[@id="shop-header"]/div[2]/div[2]/p[3]/a/text(), "")').extract())
        item["address"] = hxs.select('concat(//meta[@property="ipeen_app:address"]/@content, "")').extract()[0]
        item["opening_hours"] = self.cleanItem(hxs.select('concat(//*[@id="shop-metainfo"]/dl[2]/dd[5]/span/text(), "")').extract())
        item["off_day"] = self.cleanItem(hxs.select('concat(//*[@id="shop-metainfo"]/dl[2]/dd[6]/span/text(), "")').extract())
        item['description'] = hxs.select('concat(//meta[@name="description"]/@content, "")').extract()[0].encode('UTF-8')
        item['url'] = response.url
        item["site_name"] = hxs.select('concat(//meta[@property="og:site_name"]/@content, "")').extract()[0].encode('UTF-8')
        # item["location"] = hxs.select('//*[@id="shop-header"]/div[2]/div[2]/p[4]/a/@href').extract()[0].split("c=")[1].split("/")[0]
        # print item
        yield item

    def cleanItem(self, ex_data):
        return ex_data[0].strip() if len(ex_data) > 0 else ''

    def OnlyCharNum(self, s):
        if s:
            s2 = s.lower();
        #    fomart = 'abcdefghijklmnopqrstuvwxyz0123456789'
            fomart = '0123456789-'
            for c in s2:
                if not c in fomart:
                    int_value = s.replace(c, '')
        else:
            int_value = 0
        return int_value
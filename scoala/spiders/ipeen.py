#-*-coding:utf-8-*-
from scrapy.selector import HtmlXPathSelector
from datetime import datetime
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scoala.items import IpeenItem
from scoala.utils.select_result import list_first_item, strip_null, deduplication, clean_url
import re
from scrapy.log import ScrapyFileLogObserver, logging
from hashlib import md5
import htmlentitydefs


class IpeenSpider(CrawlSpider):
    name = 'ipeen'
    allowed_domains = ['ipeen.com.tw']
    # start_urls = ['http://www.ipeen.com.tw/']
    start_urls = [
        'http://www.ipeen.com.tw/search/taipei/d6/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d7/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d8/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d9/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d10/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d11/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d12/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d13/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d14/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d15/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d16/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d17/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d18/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d19/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d20/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d21/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d22/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d23/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d24/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d25/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d26/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d27/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d28/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d29/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d30/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d31/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d32/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d33/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d35/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taipei/d34/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d36/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d37/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d38/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d39/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d40/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d41/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d42/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d43/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d44/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d45/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d46/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d47/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d48/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d49/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d50/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d51/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d52/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d53/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d54/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d55/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d56/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d58/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d59/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d60/0-100-0-0/',
        'http://www.ipeen.com.tw/search/xinbei/d61/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d89/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d90/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d91/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d92/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d93/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d94/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d95/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d96/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d97/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d98/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d99/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d100/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d101/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d102/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d103/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d104/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d105/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d106/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d107/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d108/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d109/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d110/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d111/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d112/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d113/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d114/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d115/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d116/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d117/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d118/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d119/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taichung/d120/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d183/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d184/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d185/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d186/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d187/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d188/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d189/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d190/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d191/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d192/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d193/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d194/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d195/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d196/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d197/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d198/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d199/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d200/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d201/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d202/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d203/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d204/0-100-0-0/',
        'http://www.ipeen.com.tw/search/kaohsiung/d205/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d156/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d157/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d158/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d159/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d160/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d162/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d163/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d164/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d165/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d166/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d167/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d168/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d169/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d170/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d171/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d172/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d173/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d174/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d175/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d176/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d177/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d178/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d179/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d180/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d181/0-100-0-0/',
        'http://www.ipeen.com.tw/search/tainan/d182/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taoyuan/d67/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taoyuan/d69/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taoyuan/d70/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taoyuan/d71/0-100-0-0/',
        'http://www.ipeen.com.tw/search/taoyuan/d72/0-100-0-0/',
        'http://www.ipeen.com.tw/search/changhua/d130/0-100-0-0/',
        'http://www.ipeen.com.tw/search/changhua/d131/0-100-0-0/',
        'http://www.ipeen.com.tw/search/changhua/d132/0-100-0-0/',
        'http://www.ipeen.com.tw/search/changhua/d133/0-100-0-0/',
        'http://www.ipeen.com.tw/search/changhua/d134/0-100-0-0/',
        'http://www.ipeen.com.tw/search/changhua/d135/0-100-0-0/',
        'http://www.ipeen.com.tw/search/changhua/d136/0-100-0-0/',
        'http://www.ipeen.com.tw/search/changhua/d137/0-100-0-0/',
        'http://www.ipeen.com.tw/search/pingtung/d206/0-100-0-0/',
        'http://www.ipeen.com.tw/search/pingtung/d207/0-100-0-0/',
        'http://www.ipeen.com.tw/search/pingtung/d208/0-100-0-0/',
        'http://www.ipeen.com.tw/search/pingtung/d209/0-100-0-0/',
        'http://www.ipeen.com.tw/search/pingtung/d210/0-100-0-0/',
        'http://www.ipeen.com.tw/search/pingtung/d211/0-100-0-0/',
        'http://www.ipeen.com.tw/search/ilan/d62/0-100-0-0/',
        'http://www.ipeen.com.tw/search/ilan/d63/0-100-0-0/',
        'http://www.ipeen.com.tw/search/ilan/d64/0-100-0-0/',
        'http://www.ipeen.com.tw/search/ilan/d65/0-100-0-0/',
        'http://www.ipeen.com.tw/search/ilan/d66/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hsinchu/d73/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hsinchu/d74/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hsinchu/d75/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hsinchu/d76/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d121/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d122/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d123/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d124/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d125/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d126/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d127/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d128/0-100-0-0/',
        'http://www.ipeen.com.tw/search/nantou/d129/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hualien/d212/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hualien/d213/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hualien/d214/0-100-0-0/',
        'http://www.ipeen.com.tw/search/yunlin/d138/0-100-0-0/',
        'http://www.ipeen.com.tw/search/yunlin/d139/0-100-0-0/',
        'http://www.ipeen.com.tw/search/yunlin/d140/0-100-0-0/',
        'http://www.ipeen.com.tw/search/yunlin/d141/0-100-0-0/',
        'http://www.ipeen.com.tw/search/yunlin/d142/0-100-0-0/',
        'http://www.ipeen.com.tw/search/yunlin/d143/0-100-0-0/',
        'http://www.ipeen.com.tw/search/yunlin/d144/0-100-0-0/',
        'http://www.ipeen.com.tw/search/miaoli/d81/0-100-0-0/',
        'http://www.ipeen.com.tw/search/miaoli/d82/0-100-0-0/',
        'http://www.ipeen.com.tw/search/miaoli/d83/0-100-0-0/',
        'http://www.ipeen.com.tw/search/miaoli/d84/0-100-0-0/',
        'http://www.ipeen.com.tw/search/miaoli/d85/0-100-0-0/',
        'http://www.ipeen.com.tw/search/miaoli/d86/0-100-0-0/',
        'http://www.ipeen.com.tw/search/miaoli/d87/0-100-0-0/',
        'http://www.ipeen.com.tw/search/miaoli/d88/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hsinchucounty/d77/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hsinchucounty/d78/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hsinchucounty/d79/0-100-0-0/',
        'http://www.ipeen.com.tw/search/hsinchucounty/d80/0-100-0-0/',
        'http://www.ipeen.com.tw/search/keelung/d1/0-100-0-0/',
        'http://www.ipeen.com.tw/search/keelung/d2/0-100-0-0/',
        'http://www.ipeen.com.tw/search/keelung/d3/0-100-0-0/',
        'http://www.ipeen.com.tw/search/keelung/d4/0-100-0-0/',
        'http://www.ipeen.com.tw/search/keelung/d5/0-100-0-0-0/'
    ]

    # rules = (
    #     Rule(SgmlLinkExtractor(allow=r'Items/'), callback='parse_item', follow=True),
    # )

    def __init__(self, name=None, **kwargs):
        LOG_ERROR = "error_scrapy_%s_%s.log" % (self.name, datetime.now().strftime("%Y-%m-%d-%H:%M:%S"))
        LOG_INFO = "info_scrapy_%s_%s.log" % (self.name, datetime.now().strftime("%Y-%m-%d-%H:%M:%S"))

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
            for page in xrange(1, page_account + 1):
                # print  "p=%s" % page
                url = re.sub(r'p=\d*', 'p=%s' % page, shop_list_page[0])
                print 'get-url==', url
                yield Request(url=url, callback=self.parse_profile)

    def parse_profile(self, response):
        hxs = HtmlXPathSelector(response)

        serMain = hxs.select('//*[@id="search"]/article') # main
        # lbsResult = hxs.select('//section[contains(@class,"lbsResult")]/article') # lbsResult
        serShops = serMain.select('//div[contains(@class,"serShop")]/@id').extract() # shop_row_609926
        for shop in serShops:
            item = IpeenItem()
            shop_id = shop.split('_')[-1]
            item["id"] = shop_id
            item["domain_id"] = md5(response.url).hexdigest()
            item["page_url"] = response.url
            domain_url = response.url.split('/')[:6]
            domain_url.append('0-000-0-0')
            item["domain_url"] = '/'.join(domain_url)
            url = 'http://www.ipeen.com.tw/shop/' + '%s' % str(shop_id)
            yield Request(url=url,
                          callback=self.parse_detail,
                          meta={'item': item}
            )

    def parse_detail(self, response):
        hxs = HtmlXPathSelector(response)
        gray = hxs.select('//span[@class="mark gray"]/text()').extract()#已歇業 已搬遷
        print "gray==", gray
        # if gray and len(gray) > 0 and gray[0].strip() == '已歇業':pass
        moved = hxs.select('//div[@class="meta"]/a[@class="button small"]/text()').extract() #店家已搬移至此地點
        if len(gray) == 0:
            # print moved
            item = response.meta['item']
            print "---------------------------------------------------------------------------------"
            # --ok--
            item["name"] = hxs.select('//meta[@property="og:title"]/@content').extract()[0]
            item["price"] = self.OnlyCharNum(
                hxs.select('concat(//*[@id="shop-metainfo"]/dl[2]/dd[1]/text(), "")').extract()[0].strip())
            item["cate"] = self.cleanItem(
                hxs.select('concat(//p[@class="cate i"]/a/text(), "")').extract())
            score = hxs.select('concat(//span[@itemprop="average"]/text(), "")').extract()
            item["score"] = int(score[0].strip()) if len(score) > 0 else 0
            item['keywords'] = hxs.select('//meta[@name="keywords"]/@content').extract()[0]#.encode('UTF-8')
            item["image_url"] = hxs.select('//meta[@property="og:image"]/@content').extract()[0]
            item["longitude"] = hxs.select('//meta[@property="place:location:longitude"]/@content').extract()[0]
            item["latitude"] = hxs.select('//meta[@property="place:location:latitude"]/@content').extract()[0]
            item["type"] = hxs.select('concat(//meta[@property="og:type"]/@content, "")').extract()[0]
            # item["phone_number"] = self.cleanItem(
            #     hxs.select('concat(//*[@id="shop-header"]/div[2]/div[2]/p[3]/a/text(), "")').extract())
            item["phone_number"] = self.cleanItem(hxs.select('concat(//p[@class="tel i"]/a/text(), "")').extract())
            item["address"] = hxs.select('concat(//meta[@property="ipeen_app:address"]/@content, "")').extract()[0]
            item["opening_hours"] = self.cleanItem(
                hxs.select('concat(//*[@id="shop-metainfo"]/dl[2]/dd[5]/span/text(), "")').extract())
            item["off_day"] = self.cleanItem(
                hxs.select('concat(//*[@id="shop-metainfo"]/dl[2]/dd[6]/span/text(), "")').extract())
            item['description'] = self.strip_code(
                hxs.select('concat(//meta[@name="description"]/@content, "")').extract()[0])
            item['url'] = response.url
            # item["site_name"] = hxs.select('concat(//meta[@property="og:site_name"]/@content, "")').extract()[
            #     0]#.encode('UTF-8')
            yield item
        elif moved and len(moved) > 0 and moved[0] == '店家已搬移至此地點':
            # http://www.ipeen.com.tw/
            new_url = "http://www.ipeen.com.tw" + \
                      hxs.select('//div[@class="meta"]/a[@class="button small"]/@href').extract()[0]
            print 'get-new_url==', new_url
            yield Request(url=new_url, callback=self.parse_detail)

    def cleanItem(self, ex_data):
        return ex_data[0].strip() if len(ex_data) > 0 else ''

    def OnlyCharNum(self, s):
        if s:
            s2 = s.lower();
            fomart = '0123456789-'
            for c in s2:
                if not c in fomart:
                    int_value = s.replace(c, '')
        else:
            int_value = 0
        return int_value

    def strip_code(self, s):
        if s:
            s = re.sub(r'　', ' ', s)
            try:
                s = s.decode("utf-8")
            #                s = s.decode("gb18030")
            except Exception as e:
                print e
                return s
            html_pattern = []
            html_str = htmlentitydefs.name2codepoint
            html_pattern.append({re.compile(r"&apos;"): u"‘"})
            html_pattern.append({re.compile('&#(x)?([0-9a-fA-F]+);'): lambda result: unichr(
                int(result.group(2), result.group(1) == 'x' and 16 or 10))})

            dp = {"amp": u"＆", "quot": u'"', "ldquo": u'"', "rdquo": u'"', "lt": u'＜', "gt": u'＞', "lsquo": u"'",
                  "rsquo": u"'"}

            for k, v in html_str.items():
                vu = dp.has_key(k) and dp[k] or unichr(v)
                html_pattern.append({re.compile(r"&%s;" % (k)): vu})

            html_pattern.append({re.compile(r'(\t){2,}'): r'\t'})
            html_pattern.append({re.compile(r'(\r){2,}'): r'\r'})
            html_pattern.append({re.compile(r'(\n){2,}'): r'\n'})
            html_pattern.append({re.compile(r'( ){2,}'): u' '})
            html_pattern.append({re.compile(r'^(\n)|(\n)$'): ''})

            for i in html_pattern:
                for k, v in i.items():
                    s = k.sub(v, s)
            return s.encode("utf-8")
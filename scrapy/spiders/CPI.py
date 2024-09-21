import scrapy, random
from scrapy.crawler import CrawlerProcess
from bank.userAgents import RANDOM
from bank.rotatingProxy import PROXY

class CPIItem(scrapy.Item):
    Year = scrapy.Field()
    Jan = scrapy.Field()
    Feb = scrapy.Field()
    Mar = scrapy.Field()
    Apr = scrapy.Field()
    May = scrapy.Field()
    June = scrapy.Field()
    July = scrapy.Field()
    Aug = scrapy.Field()
    Sep = scrapy.Field()
    Oct = scrapy.Field()
    Nov = scrapy.Field()
    Dec = scrapy.Field()
    Avg = scrapy.Field()

class CPISpider(scrapy.Spider):
    name = "cpi"
    base = "https://www.usinflationcalculator.com"

    def start_requests(self):
        url = self.base + "/inflation/consumer-price-index-and-annual-percent-changes-from-1913-to-2008/"
        yield scrapy.Request(
            url=url, meta={"proxy": PROXY}, callback=self.parse_table
        )
    
    def parse_table(self, response):
        item = CPIItem()
        
        convert = {
            0: "Jan",
            1: "Feb",
            2: "Mar",
            3: "Apr",
            4: "May",
            5: "June",
            6: "July",
            7: "Aug",
            8: "Sep",
            9: "Oct",
            10: "Nov",
            11: "Dec",
            12: "Avg",
        }

        year = 1913

        rows = response.css('div.td-page-content.tagdiv-type tr')
        for row in rows[2:-1]:
            cols = row.css('td::text').getall()[:13]

            item['Year'] = year
            year += 1
            
            for i in range(13):
                item[convert[i]] = cols[i]

            yield item

process = CrawlerProcess(
    settings={
        "FEEDS": {
            "CPI.csv": {
                "format": "csv",
                "overwrite": True,
            },
        },
        "FEED_EXPORT_FIELDS": ["Year", 
                                "Jan", 
                                "Feb", 
                                "Mar", 
                                "Apr", 
                                "May", 
                                "June", 
                                "July", 
                                "Aug", 
                                "Sep", 
                                "Oct", 
                                "Nov", 
                                "Dec", 
                                "Avg"],
        "USER_AGENT": random.choice(RANDOM),
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "FEED_EXPORT_ENCODING": "utf-8",
        "DNS_TIMEOUT": 120,
        "ROBOTSTXT_OBEY": False,
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }
)

process.crawl(CPISpider)
process.start()

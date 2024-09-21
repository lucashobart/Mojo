import scrapy, random
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from bank.userAgents import RANDOM
from bank.rotatingProxy import PROXY

class FranchiseItem(scrapy.Item):
    franchise = scrapy.Field()
    releases = scrapy.Field()
    titles = scrapy.Field()
    totalGross = scrapy.Field()
    

class FranchiseSpider(scrapy.Spider):
    name = "franchise"
    base = "https://www.boxofficemojo.com"

    def start_requests(self):
        url = "https://www.boxofficemojo.com/franchise/?ref_=bo_lnav_hm_shrt"
        yield scrapy.Request(
            url=url, meta={"proxy": PROXY}, callback=self.parse_table
        )
    
    def parse_table(self, response):
        rows = response.css('div.a-section.imdb-scroll-table-inner tr')
        for row in rows[1:]:
            details = row.css('td::text').getall()
            numReleases = int(details[1])
            if numReleases > 1:
                totGross = int(self.cleanNum(details[0]))
                perMovieAvg = totGross / numReleases
                if perMovieAvg > 10000000:
                    post_url = row.css('td a::attr(href)').get()
                    yield scrapy.Request(
                        url=self.base+post_url, meta={"proxy": PROXY}, callback=self.parse_franchise_page
                    )
        
    def parse_franchise_page(self, response):
        item = FranchiseItem()

        header = response.css('h1.mojo-gutter::text').get()
        item['franchise'] = header[header.index(" ")+1:]

        totalGross, numReleases, titles = 0, None, []
        seen = set()
        
        rows = response.css('div.a-section.imdb-scroll-table-inner tr')
        for row in rows[1:]:
            title, date = row.css('td a::text').getall()[:2]
            
            strGross = self.cleanNum(row.css('td::text').getall()[1])
            if not strGross.isdigit():
                break

            gross = int(strGross)
            
            if title in seen:
                curr_perMovieAvg = totalGross / int(numReleases)
                if gross < (0.15 * curr_perMovieAvg):
                    break
            
            seen.add(title)
            numReleases = row.css('td::text').get()
            totalGross += gross
            date = self.convert_date(" ".join(date.split(" ")[:3]))
            titles.append(title + " > " + date)
        
        item['releases'] = numReleases
        item['titles'] = " | ".join(titles)
        item['totalGross'] = str(totalGross)

        yield item
 
    def cleanNum(self, strNum):
        return ''.join([char for char in strNum if char.isdigit()])

    def convert_date(self, date_str: str) -> str:
        input_format = "%b %d, %Y"  # Example: "Jan 1, 2024"
        output_format = "%m-%d-%Y"  # Example: "01-01-2024"
        
        date_obj = datetime.strptime(date_str, input_format)
        
        formatted_date = date_obj.strftime(output_format)
        return formatted_date

process = CrawlerProcess(
    settings={
        "FEEDS": {
            "franchises2.csv": {
                "format": "csv",
                "overwrite": True,
            },
        },
        "FEED_EXPORT_FIELDS": ["franchise", 
                               "releases", 
                               "totalGross", 
                               "titles"],
        "USER_AGENT": random.choice(RANDOM),
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "FEED_EXPORT_ENCODING": "utf-8",
        "DNS_TIMEOUT": 120,
        "ROBOTSTXT_OBEY": False,
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }
)

process.crawl(FranchiseSpider)
process.start()

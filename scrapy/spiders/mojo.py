import scrapy, random
from scrapy.crawler import CrawlerProcess
from bank.userAgents import RANDOM
from bank.rotatingProxy import PROXY

class MojoItem(scrapy.Item):
    title = scrapy.Field()
    domesticGross = scrapy.Field()
    url = scrapy.Field()
    distributor = scrapy.Field()
    openingBO = scrapy.Field()
    openingTheaters = scrapy.Field()
    releaseDate = scrapy.Field()
    MPAA = scrapy.Field()
    runtime = scrapy.Field()
    genre = scrapy.Field()
    theatricalWindow = scrapy.Field()
    widestRelease = scrapy.Field()

class MojoSpider(scrapy.Spider):
    name = "mojo"
    base = "https://www.boxofficemojo.com"

    def start_requests(self):
        getURL = lambda yr: f"https://www.boxofficemojo.com/year/world/{yr}/"
        start_yr, end_yr = 1977, 2023
        for year in range(start_yr, end_yr + 1):
            yield scrapy.Request(
                url=getURL(year), meta={"proxy": PROXY}, callback=self.parse_table
            )
    
    def parse_table(self, response):
        rows = response.css('div.a-section.imdb-scroll-table-inner tr')
        for row in rows[1:]:
            # no domestic release
            if row.css('td.a-text-right.mojo-field-type-money::text')[1].get() == "-":
                continue
            movieLink = self.base + row.css('a.a-link-normal::attr(href)').get()
            yield scrapy.Request(
                url=movieLink, meta={"proxy": PROXY}, callback=self.parse_movie_page
            )
        
    def parse_movie_page(self, response):
        domLink = self.base + response.css('table.a-bordered.a-horizontal-stripes.mojo-table.releases-by-region td a::attr(href)').get()
        yield scrapy.Request(
            url=domLink, meta={"proxy": PROXY}, callback=self.parse_data
        )
    
    def parse_data(self, response):
        item = MojoItem()
        
        item['title'] = response.css('h1.a-size-extra-large::text').get()
        item['url'] = response.url
        item['domesticGross'] = self.cleanNum(response.css('div.a-section.a-spacing-none.mojo-performance-summary-table span.money::text').get())
        
        details = response.css('div.a-section.a-spacing-none.mojo-summary-values div')
        for detail in details:
            
            text = detail.css('span::text').getall()
            cat = text[0]
            
            if 'Distributor' in cat:
                item['distributor'] = text[1]
            elif 'Opening' in cat:
                if len(text) == 3:
                    item["openingTheaters"] = self.cleanNum(text[2])
                item["openingBO"] = self.cleanNum(text[1])
            elif 'Release Date' in cat:
                item['releaseDate'] = detail.css('span a::text').get()
            elif 'MPAA' in cat:
                item[cat] = text[1]
            elif 'Running Time' in cat:
                rt = text[1].split(" ")
                minutes, prev = 0, rt[0]
                for el in rt[1:]:
                    if el.isdigit():
                        prev = el
                    else:
                        minutes += int(prev) if el == 'min' else (int(prev) * 60)
                item['runtime'] = minutes
            elif 'Genre' in cat:
                words = []
                for el in text[1].split("\n"):
                    new = el.strip()
                    if len(new):
                        words.append(new)
                item['genre'] = ",".join(words)
            elif 'In Release' in cat:
                item['theatricalWindow'] = text[1].split(" ")[0]
            elif 'Widest Release' in cat:
                item['widestRelease'] = self.cleanNum(text[1])

        yield item
    
    def cleanNum(self, strNum):
        return ''.join([char for char in strNum if char.isdigit()])

process = CrawlerProcess(
    settings={
        "FEEDS": {
            "mojo.csv": {
                "format": "csv",
                "overwrite": True,
            },
        },
        "FEED_EXPORT_FIELDS": ["title", 
                               "distributor", 
                               "releaseDate", 
                               "runtime", 
                               "MPAA", 
                               "genre", 
                               "openingBO", 
                               "openingTheaters", 
                               "widestRelease", 
                               "theatricalWindow",
                               "url", 
                               "domesticGross"],
        "USER_AGENT": random.choice(RANDOM),
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "FEED_EXPORT_ENCODING": "utf-8",
        "DNS_TIMEOUT": 120,
        "ROBOTSTXT_OBEY": False,
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }
)

process.crawl(MojoSpider)
process.start()

from scrapy import cmdline
cmdline.execute("scrapy crawl resident -o ResidentCrawler/data/overall_events.csv".split())

import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

dow_30 = pd.read_csv("/Users/Kevin/Desktop/Dow30.csv")

cols = ['Firm','Date','A/D', 'Owner','Shares','Holdings','Category','Security']
transactions = []

class InsiderSpider(scrapy.Spider):
    
    name = "insider"
    start_urls = []
    
    for cik in dow_30.iloc[:,2]:
        
        for x in range(0,5):
            
            start_urls.append('https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK=' + str(cik) + "&type=&dateb=&owner=include&start=" + str((x * 80)))       
    
    def parse(self, response):
        
        for row in response.css('table#transaction-report > tr'):

            ad = row.css(':nth-child(1)::text').get()
            date = row.css(':nth-child(2)::text').get()
            owner = row.css(':nth-child(4)::text').get()
            shares = row.css(':nth-child(8)::text').get()
            hold = row.css(':nth-child(9)::text').get()
            category = row.css(':nth-child(6)::text').get()
            security = row.css(':nth-child(12)::text').get()
            firm = response.css('title::text').get()
            transactions.append([date,firm.replace("Ownership Information: ",""),ad,owner,shares,hold,category,security])
                   
            
process = CrawlerProcess()
process.crawl(InsiderSpider)
process.start()

df = pd.DataFrame(transactions, columns=cols)
df = df[df.Owner != "Reporting Owner"]
df.to_csv('/Users/Kevin/Desktop/Dow_30_Insider_Activity.csv', index = None, header=False)






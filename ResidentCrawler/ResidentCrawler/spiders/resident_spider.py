import csv
import datetime
import os
from builtins import *

import scrapy
from .storage import connect
from .item import ResidentItem


class ResidentSpider(scrapy.Spider):
    name = "resident"

    def start_requests(self):

        urls = []

        # yyyy, m, d
        start_date = datetime.date(2019, 2, 15)
        end_date = datetime.date.today()

        for dt in self.get_date_range(start_date, end_date):
            urls.append('https://www.residentadvisor.net/events/de/berlin/day/' + dt.strftime("%Y-%m-%d"))

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for event in response.css('div.bbox'):
            event_item = ResidentItem()
            event_item['event_day'] = response.css('p.eventDate > a > span::text').extract_first()[0:3]
            event_item['event_date'] = str(
                self.replace_short_month(response.css('p.eventDate > a > span::text').extract_first()[5:16]))
            event_item['club_name'] = event.css('h1.event-title > span > a::text').extract()
            event_item['event_attending'] = event.css('p.attending > span::text').extract()
            yield event_item

    def closed(self, reason):
        self.clear_files()
        print("Data files cleared")
        self.logger.info("Spider" + reason)
        self.customize_csv()
        print("CSV customized")
        self.upload_to_database()
        print("Data uploaded to Database")


    @staticmethod
    def clear_files():

        directory = os.path.dirname(os.path.realpath('__file__'))

        input_file = open(os.path.join(directory, 'ResidentCrawler/data/overall_events.csv'), "w")
        output_file = open(os.path.join(directory, 'ResidentCrawler/data/customized_overall_events.csv'), "w")
        input_file.truncate()
        output_file.truncate()
        input_file.close()
        output_file.close()

    @staticmethod
    def get_date_range(date1, date2):
        for n in range(int((date2 - date1).days) + 1):
            yield date1 + datetime.timedelta(n)

    @staticmethod
    def replace_short_month(month):

        months = {'Jan': 'January', 'Feb': 'February', 'Mar': 'March', 'Apr': 'April', 'Jun': 'June', 'Jul': 'July',
                  'Aug': 'August', 'Sep': 'September', 'Oct': 'October', 'Nov': 'November', 'Dec': 'December'}

        for key, value in months.items():
            month = month.replace(key, value)

        converted_month = datetime.datetime.strptime(month, '%d %B %Y').strftime('%Y-%m-%d')

        return converted_month

    @staticmethod
    def customize_csv():

        directory = os.path.dirname(os.path.realpath('__file__'))

        with open(os.path.join(directory, 'ResidentCrawler/data/overall_events.csv')) as input_file, open(
                os.path.join(directory, 'ResidentCrawler/data/customized_overall_events.csv'), 'w', newline='') as output_file:
            writer = csv.writer(output_file)
            reader = csv.reader(input_file)
            for row in reader:
                if len(row[1]) != 0 and len(row[3]) != 0:
                    if any(field.strip() for field in row):
                        writer.writerow(row)

    @staticmethod
    def upload_to_database():

        directory = os.path.dirname(os.path.realpath('__file__'))

        cnx = connect()

        csv_data = csv.reader(open(os.path.join(directory, 'ResidentCrawler/data/customized_overall_events.csv')))

        cursor = cnx.cursor()

        for row in csv_data:
            cursor.execute(
                'INSERT INTO overall_fount(club_name,event_day,event_date,event_attending)VALUES("%s", "%s", "%s", "%s")' % (
                row[0], row[1], row[2], row[3]))

        cursor.execute(
            '''UPDATE overall_fount SET club_name = REPLACE(club_name, "\'", ""), event_day = REPLACE(event_day, "\'", ""), event_date = REPLACE(event_date, "\'", ""), event_attending = REPLACE(event_attending, "\'", "")''')

        cursor.execute('DELETE FROM overall_fount WHERE club_name="club_name"')

        cnx.commit()
        cursor.close()

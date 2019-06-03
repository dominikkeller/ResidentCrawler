# -*- coding: utf-8 -*-

import scrapy


class ResidentItem(scrapy.Item):
    # define the fields for your item here like:
    event_day = scrapy.Field()
    event_date = scrapy.Field()
    club_name = scrapy.Field()
    event_attending = scrapy.Field()
    pass

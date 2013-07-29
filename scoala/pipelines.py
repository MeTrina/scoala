# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

import sqlite3
from os import path
import pymongo
from scrapy import log
from scrapy.exceptions import DropItem
from twisted.enterprise import adbapi

import time
import MySQLdb.cursors
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.contrib.conf import mysql_server


class SQLiteStorePipeline(object):
    filename = 'data.sqlite'

    def __init__(self):
        self.conn = None
        dispatcher.connect(self.initialize, signals.engine_started)
        dispatcher.connect(self.finalize, signals.engine_stopped)

    def process_item(self, domain, item):
        self.conn.execute('insert into blog values(?,?,?)',
                          (item.url, item.raw, unicode(domain)))
        return item

    def initialize(self):
        if path.exists(self.filename):
            self.conn = sqlite3.connect(self.filename)
        else:
            self.conn = self.create_table(self.filename)

    def finalize(self):
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
            self.conn = None

    def create_table(self, filename):
        conn = sqlite3.connect(filename)
        conn.execute("""create table blog
                     (url text primary key, raw text, domain text)""")
        conn.commit()
        return conn


class MySQLStorePipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect(host=mysql_server['host'], user=mysql_server['user'],
                                    passwd=mysql_server['pwd'], db='data_mining_xcj', charset=mysql_server['charset'])
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if item:
            try:
                self.cursor.execute("INSERT ignore INTO  ipeen_shop ( \
                id, name, cate, price, score, keywords, image_url, longitude, \
                latitude, type, phone_number, address, opening_hours, off_day, description, \
                url, domain_id, domain_url, page_url) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                    (
                                        item["id"], item["name"], item["cate"], item["price"],
                                        item["score"], item['keywords'], item["image_url"],
                                        item["longitude"], item["latitude"], item["type"], item["phone_number"],
                                        item["address"], item["opening_hours"], item["off_day"], item['description'],
                                        item['url'], item['domain_id'], item['domain_url'], item['page_url']
                                    )
                )
                self.conn.commit()
            except MySQLdb.Error, e:
                print "Error %d: %s" % (e.args[0], e.args[1])
            return item


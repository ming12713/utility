#!/usr/bin/env python
import bson
import pymongo
from pymongo import MongoClient
from pymongo import ReadPreference
import sys

import unicodedata
from bson import ObjectId
from bson.errors import InvalidId

reload(sys)
sys.setdefaultencoding('utf-8')
CONN_ADDR2 = "112.124.2.139:27018"
username = 'root'
password = 'Fosun@1234'
admin = 'admin'

def getDBlist(m_db):
    client = MongoClient(CONN_ADDR2)
    client.the_database.authenticate(username,password,source=admin)

    db = client.get_database(m_db,read_preference=ReadPreference.SECONDARY)
    collectionlist = db.collection_names()

    for collection in collectionlist:
      #  coll_context = db[collection].find({"fosun_create_date_day": {'$gt': "20160908"}})
      #   coll_context = db[collection].find().sort("fosun_create_date_day",pymongo.DESCENDING).limit(1)
        coll_context = db[collection].find().limit(1)
        coll_count = coll_context.count()
        try:
            for i in coll_context:
                print "\n %-10s" %collection,i["fosun_import_mode"],i['fosun_create_date_day']

        except Exception,e:
            print e
mdbs = ['rawdata']
for ddb in mdbs:
    cdb = getDBlist(ddb)


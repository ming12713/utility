#!/usr/bin/env python
import os
import re
from pymongo import MongoClient
from pymongo import ReadPreference
import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')
CONN_ADDR1 = "x.x.x.x:27017"
CONN_ADDR2 = "x.x.x.x:27018"
username = 'root'
password = 'xxxxx
admin = 'admin'
dumpdir = '/tmp/mongo'
nowdate = datetime.datetime.now() - datetime.timedelta(days=1)
dumpdate = nowdate.strftime("%Y%m%d")
def getcolls(m_db):
    client = MongoClient(CONN_ADDR2)
    client.the_database.authenticate(username,password,source=admin)
    db = client.get_database(m_db,read_preference=ReadPreference.SECONDARY)
    collectionlist = db.collection_names()
    v_list=[]
    for collection in collectionlist:
        coll_context = db[collection].find().limit(1)

        try:
            for i in coll_context:
                mathobj = re.search(r'^[0-9]{1,10}',i['fosun_create_date_day'])
                if mathobj:
                    v_list.append(collection)
        except Exception,e:
            print e
    return v_list

def dump(cdb):
    aliyun_coll = getcolls(cdb)
    for table in aliyun_coll:
            args = '\'{"fosun_create_date_day":{"$gt":' + dumpdate + '}}\''
            cmd='mongodump'
            c_args=('-h',CONN_ADDR2,'--authenticationDatabase',admin,'-u',username,'-p',password,'-d',cdb,'-c',table,'--query',args)
            for i in range(len(c_args)):
                  cmd = cmd + " " + str(c_args[i])
            os.chdir(dumpdir)
            os.system(cmd)

def restore(cdb):
    # client = MongoClient(CONN_ADDR1)
    # client.the_database.authenticate(username, password, source=admin)
    # db =client[cdb]
     cmd='mongorestore'
     c_args=('-h',CONN_ADDR1,'--authenticationDatabase',admin,'-u',username,'-p',password)
     for i in range(len(c_args)):
         cmd = cmd + " " + str(c_args[i])
     os.chdir('/tmp/mongo')
     os.system(cmd)

def main():
    mdbs = ['fonova','fonova_test']
    for ddb in mdbs:
       dump_dump = dump(ddb)
    for rdb in mdbs:
        restore_restore = restore(rdb)

if __name__ == '__main__':
    main()

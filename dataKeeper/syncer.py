#!/usr/local/bin/python
#coding=utf-8
# Author: 568190317@qq.com liumeng

import os,redis,sys,re
import MySQLdb
import time

MYSQL_PATH = "/usr/local/bin/mysql"
MYSQL_HOST = "10.6.9.149"
MYSQL_USER = "root"
MYSQL_PWD = "root"
MYSQL_PORT = 3306
MYSQL_DB = "liumeng"

REDIS_HOST = "10.6.9.149"
REDIS_PORT = "6379"
REDIS_DB = "3"

pageid_regx = re.compile("^\d{1,10}$")

if __name__ == "__main__":

	r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
	
	try:
		conn = MySQLdb.connect(host=MYSQL_HOST,user=MYSQL_USER,passwd=MYSQL_PWD,port=MYSQL_PORT,db=MYSQL_DB)
		print "Database connected..."
		cur = conn.cursor()
	
		while 1:
			
			time.sleep(10)
			
			arr = r.keys()

			for sKey in arr:
				pageType = 'n'
				pageid = 0
				pv = 0
				uv = 0	
				store_pv = 0
				store_uv = 0
			
				keyType = r.type(sKey)
				if keyType != "hash":
					continue
	
				keyArr = sKey.split("_")
				if len(keyArr) != 2:
					continue
				pageType = keyArr[0]
				pageId = keyArr[1]

				if not pageid_regx.match(pageId):
					continue
				pageId = long(pageId)
			
				pv = int(r.hget(sKey,"pv"))	
				uv = int(r.hget(sKey,"uv"))	
				store_pv = int(r.hget(sKey,"store_pv"))	
				store_uv = int(r.hget(sKey,"store_uv"))	

				tableName =""
				if pageType == "g":
					tableName = "goods_pv"
				elif pageType == "f":
					tableName = "features_pv"
				else:
					continue

				inSql="REPLACE INTO "+ tableName +" (id,pv,uv,store_pv,store_uv) VALUES(%s,%s,%s,%s,%s)";
				value=[pageId,pv,uv,store_pv,store_uv]
				cur.execute(inSql,value)

				print "Insert:" + inSql

				'''
				values=[]
				for i in range(20):
				values.append()
				cur.executemany(inSql,values)
				'''
				conn.commit()

		cur.close()
		conn.close()

	except MySQLdb.Error,e:
		print "Mysql Error %d: %s" % (e.args[0], e.args[1])


	



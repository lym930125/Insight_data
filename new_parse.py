from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import unix_timestamp
import time
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim
from pyspark.sql.functions import lit
from mysql.connector import errorcode
import mysql.connector
sc = SparkContext(appName="migrate")
sqlContext = SQLContext(sc)

sites_list = []
version_list = []
site_version_list = []
version_data_dict = {}
temp_dict = {}
count = 0
start_flag=False
content_flag = False

USER = ''
PASS = ''
HOST = ''
DB = ''

try:
        cnx = mysql.connector.connect(user=USER, password=PASS,
                                host=HOST,
                                database=DB)
        cursor = cnx.cursor()
	cursor.execute("select count(*) from sites")
	result = cursor.fetchall()
	site_count = result[0][0]
	cursor.execute("select count(*) from version_site")
	result = cursor.fetchall()
	version_count = result[0][0]
	
except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
        else:
                print(err)

def get_data_content(line):
        line_list=line.strip().split(' ')
        return line_list[1]

def insert_into_mysql():
	USER = ''
	PASS = ''
	HOST = ''
	DB = ''
	try:
        	cnx = mysql.connector.connect(user=USER, password=PASS,
                	                host=HOST,
                        	        database=DB)
        	cursor = cnx.cursor()
        
	except mysql.connector.Error as err:
        	if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                	print("Something is wrong with your user name or password")
        	elif err.errno == errorcode.ER_BAD_DB_ERROR:
                	print("Database does not exist")
        	else:
                	print(err)
	stmt = "insert into sites(site_id,site_url) values(%s,%s)"
	cursor.executemany(stmt,sites_list)
#	cursor.execute(stmt)
	v_list =[]
	for d in version_list:
		temp = []
		temp.append(d['id'])
		temp.append(d['version_date'])
		temp.append(d['content_length'])
		temp.append(d['s3_link'])
		temp.append(d['zip_file'])
		v_list.append(temp)
	stmt2 = "insert into version_site(site_id,version_site_id) values(%s,%s)"
	cursor.executemany(stmt2,site_version_list)	
#	cursor.execute(stmt2)
	
	stmt3 = "insert into version(version_site_id,version_date,content_length,s3_link,zip_file) values (%s,%s,%s,%s,%s)"

	cursor.executemany(stmt3,v_list)
	cnx.commit()
	cnx.close()
	
def obtain_data(data,link_str):
	global count
	global sites_list
	global version_list
	global version_data_dict
	global site_count
	global version_count
	global site_version_list
	global temp_dict
	if(count==100):
		insert_into_mysql()
		count = 0
		sites_list = []
		version_list = []
		site_version_list=[]
	try:
		line=data.value.encode('utf-8')
	except:
		pass
	global start_flag
	global content_flag

	if "WARC/" in line:
		if  bool(version_data_dict):
               		content_flag=False
			site_version_list.append([temp_dict['site_id'],temp_dict['version_id']])
                      	version_data_dict['s3_link']="s3a://commoncrawl/crawl-data/CC-MAIN-2018-22/wet.paths.gz"
                      	version_data_dict['zip_file']=link_str
                        version_list.append(version_data_dict)
                        version_data_dict = {}
                        count+=1

	if start_flag == False:
        	pass
	else:	
                if content_flag == True:
                	pass
                else:
                	if "WARC-Target-URI" in line:
				site_id = str(hash(get_data_content(line)))
				sites_list.append([site_id,get_data_content(line)])
				temp_dict['site_id']=site_id
				
			
				
                  	elif "WARC-Date" in line:
				date_time=get_data_content(line).split('T')
                                version_data_dict['version_date']=(date_time[0])
			elif "WARC-Record-ID" in line:
                            	version_data_dict['id']=str(hash(get_data_content(line)))
				temp_dict['version_id']=version_data_dict['id']
                        elif "Content-Length:" in line:
				version_data_dict['content_length']=(get_data_content(line))
				content_flag=True
			else:
                        	pass
	
	if "publisher:" in line:
		start_flag=True

alldata = sc.textFile("s3a://commoncrawl/crawl-data/CC-MAIN-2018-22/wet.paths.gz")
data_list = alldata.collect()
length = len(data_list)
for i in range(0,length):
	f = open("count.txt","a")
	f.write(str(i))
	f.close()
	print(str(i)+"th file is inserting")
	link_str = str(data_list[i])
	link = "s3a://commoncrawl/"+link_str
	lines = sqlContext.read.text(link)
	global start_flag
	start_flag = False
	global content_flag
	content_flag = False
	lines.foreach(lambda x: obtain_data(x,link_str))	

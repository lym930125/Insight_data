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
SQL_USER = ''
SQL_PWD = ''
SQL_HOST = ''
REDSHIFT_LINK = ''


sc = SparkContext(appName="migrate")
sqlContext = SQLContext(sc)
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "")
sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", "")

sites = sqlContext.read.format("jdbc")\
	.options(url=SQL_HOST,\
		driver = "com.mysql.jdbc.Driver",\
		dbtable="sites",\
		user=SQL_USER,\
		password=SQL_PWD)\
		.load()

version_site = sqlContext.read.format("jdbc")\
	.options(url=SQL_HOST,\
		driver = "com.mysql.jdbc.Driver",\
		dbtable="version_site",\
		user=SQL_USER,\
		password=SQL_PWD)\
		.load()

version = sqlContext.read.format("jdbc")\
		.options(url=SQL_HOST,\
			driver = "com.mysql.jdbc.Driver",\
			dbtable="version",\
			user=SQL_USER,\
			password=SQL_PWD)\
			.load()

status = sqlContext.read.format("jdbc")\
                .options(url=SQL_HOST,\
                        driver = "com.mysql.jdbc.Driver",\
                        dbtable="status",\
                        user=SQL_USER,\
                        password=SQL_PWD)\
                        .load()

version_site.createOrReplaceTempView("version_site")
status.createOrReplaceTempView("status")

new_site = spark.sql("select site_id,version_site_id as version_id from version_site where version_site_id not in (select version_id from status)")


new_site_to_mysql = new_site.withColumn('status',lit('in progress'))
new_site_to_mysql.write.format('jdbc').options(
      url=SQL_HOST,
      driver='com.mysql.jdbc.Driver',
      dbtable='status',
      user=SQL_USER,
      password=SQL_PWD).mode('append').save()

redshift_site = spark.sql("select site_id,version_id as version_site_id from status where status = 'in progress'")
#redshift_site = redshift_site.withColumn('version_id',redshift_site['version_site_id'])
#redshift_site = redshift_site.drop(redshift_site['version_site_id'])
#redshift_site.show()

redshift_site.write\
  	.format("com.databricks.spark.redshift")\
  	.option("url", REDSHIFT_LINK)\
  	.option("dbtable", "version_site")\
	.option("tempformat","CSV")\
  	.option("tempdir", "s3n://tempdata666")\
  	.option("forward_spark_s3_credentials","true")\
	.mode("append")\
  	.save()


#redshift_site.write.format('jdbc').options(driver='com.amazon.redshift.jdbc42.Driver',url='jdbc:redshift://insight.cxlg3frcajlc.us-east-1.redshift.amazonaws.com:5439/dev',dbtable='version_s',user='insight',password='Insight2018').mode('append').save()


sites_redshift = redshift_site.join(sites,"site_id")
sites_version_redshift = sites_redshift.join(version,"version_site_id")

sites_version_redshift.createOrReplaceTempView("new_data")
#new_data = spark.sql("select site_url,record_id,content_length,version_date from sites s,version v,insert_data id where s.site_id = id.site_id and v.version_site_id=id.version_site_id") 
new_data= spark.sql("select site_url,content_length,version_date,s3_link,zip_file from new_data")



formatted_new_data = new_data.withColumn('site_url',new_data['site_url'].cast('string'))

#formatted_new_data.write.format('jdbc').options(driver='com.amazon.redshift.jdbc42.Driver',url='jdbc:redshift://insight.cxlg3frcajlc.us-east-1.redshift.amazonaws.com:5439/dev',dbtable='data',user='insight',password='Insight2018').mode('append').save()

formatted_new_data.show()
formatted_new_data.write\
        .format("com.databricks.spark.redshift")\
        .option("url", REDSHIFT_LINK)\
        .option("dbtable", "data")\
        .option("tempdir", "s3n://tempdata666")\
	.option("tempformat","CSV")\
        .option("forward_spark_s3_credentials","true")\
        .mode("append")\
        .save()

'''
new_site_done = new_site.withColumn('status',lit('done'))
new_site_done = new_site_done.withColumn('version_id',new_site_done['version_site_id'])
new_site_done = new_site_done.drop(new_site_done['version_site_id'])

new_site_done.write.format('jdbc').options(
      url='jdbc:mysql://54.208.116.206:3306/webdata',
      driver='com.mysql.jdbc.Driver',
      dbtable='status',
      user='root',
      password='insight').mode('overwrite').save()
'''

'''
version_site.createOrReplaceTempView("version_site")
version_site_redshift = spark.sql("select site_id,version_id from version_site")

version_site_redshift.write.format('jdbc').options(driver='com.amazon.redshift.jdbc42.Driver',url='jdbc:redshift://insight.cxlg3frcajlc.us-east-1.redshift.amazonaws.com:5439/dev',dbtable='version_site',user='insight',password='Insight2018').mode('append').save()

site_version_site = sites.join(version_site,"site_id")
site_version_site_version = site_version_site.join(version,"version_site_id")
site_version_site_version.createOrReplaceTempView("web")
#cas_site = spark.sql("select site_url,record_id,content,content_length,version_date from web")
#cas_site = cas_site.withColumn('site_url',cas_site['site_url'].cast('string'))
#cas_site = cas_site.withColumn('content',cas_site['content'].cast(StringType()))
cas_site = spark.sql("select site_url,record_id,content_length,version_date from web")
cas_site.show()
cas_site = cas_site.withColumn('site_url',trim(cas_site['site_url']).cast(StringType()))
#cas_site = cas_site.withColumn('content',trim(cas_site['content']).cast('string'))

#cas_site.show()

cas_site.write.format('jdbc').options(driver='com.amazon.redshift.jdbc42.Driver',url='jdbc:redshift://insight.cxlg3frcajlc.us-east-1.redshift.amazonaws.com:5439/dev',dbtable='data',user='insight',password='Insight2018').mode('append').save()

#rdd.saveToCassandra("webdata","site")
#rdd.saveToCassandra("webdata","site")
#rdd = cas_site.rdd.map(tuple)
#rdd.saveToCassandra("webdata","site")
'''


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

## Declare the variables of connection information
SQL_USER = ''
SQL_PWD = ''
SQL_HOST = ''
REDSHIFT_LINK = ''

##Create a Spark instances
sc = SparkContext(appName="migrate")
sqlContext = SQLContext(sc)
spark = SparkSession \
    .builder \
    .appName("Migration") \
    .getOrCreate()

##Set access_key for the connection of redshift
sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "")
sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", "")


##Get the DataFrame of sites table from MySQL
sites = sqlContext.read.format("jdbc")\
	.options(url=SQL_HOST,\
		driver = "com.mysql.jdbc.Driver",\
		dbtable="sites",\
		user=SQL_USER,\
		password=SQL_PWD)\
		.load()


## Get the DateFrame of version_site table from MySQL
version_site = sqlContext.read.format("jdbc")\
	.options(url=SQL_HOST,\
		driver = "com.mysql.jdbc.Driver",\
		dbtable="version_site",\
		user=SQL_USER,\
		password=SQL_PWD)\
		.load()

## Get the DataFrame of version table from MySQL
version = sqlContext.read.format("jdbc")\
		.options(url=SQL_HOST,\
			driver = "com.mysql.jdbc.Driver",\
			dbtable="version",\
			user=SQL_USER,\
			password=SQL_PWD)\
			.load()

## Get the DataFrame of status table from MySQL
status = sqlContext.read.format("jdbc")\
                .options(url=SQL_HOST,\
                        driver = "com.mysql.jdbc.Driver",\
                        dbtable="status",\
                        user=SQL_USER,\
                        password=SQL_PWD)\
                        .load()

## Create the View of version_site and status to get the data from this 2 tables
version_site.createOrReplaceTempView("version_site")
status.createOrReplaceTempView("status")

## select the data needed for table "status" in MySQL
new_site = spark.sql("select site_id,version_site_id as version_id from version_site where version_site_id not in (select version_id from status)")

## add an column showing the current migration status for the website.
new_site_to_mysql = new_site.withColumn('status',lit('in progress'))

## write the column with current status to MySQL status table
new_site_to_mysql.write.format('jdbc').options(
      url=SQL_HOST,
      driver='com.mysql.jdbc.Driver',
      dbtable='status',
      user=SQL_USER,
      password=SQL_PWD).mode('append').save()

## select the data needed for table "version_site" in Redshift
redshift_site = spark.sql("select site_id,version_id as version_site_id from status where status = 'in progress'")


## Insert the data into "version_site" table in Redshift
redshift_site.write\
  	.format("com.databricks.spark.redshift")\
  	.option("url", REDSHIFT_LINK)\
  	.option("dbtable", "version_site")\
	.option("tempformat","CSV")\
  	.option("tempdir", "s3n://tempdata666")\
  	.option("forward_spark_s3_credentials","true")\
	.mode("append")\
  	.save()

## Join 3 tables in MySQL together and do the denormalization
sites_redshift = redshift_site.join(sites,"site_id")
sites_version_redshift = sites_redshift.join(version,"version_site_id")
sites_version_redshift.createOrReplaceTempView("new_data")

## Select and edit the dataframe needed for "data" table in Redshift
new_data= spark.sql("select site_url,content_length,version_date,s3_link,zip_file from new_data")
formatted_new_data = new_data.withColumn('site_url',new_data['site_url'].cast('string'))

## Insert the data into "data" table in Redshift
formatted_new_data.write\
        .format("com.databricks.spark.redshift")\
        .option("url", REDSHIFT_LINK)\
        .option("dbtable", "data")\
        .option("tempdir", "s3n://tempdata666")\
	.option("tempformat","CSV")\
        .option("forward_spark_s3_credentials","true")\
        .mode("append")\
        .save()



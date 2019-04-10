import os
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from datetime import date, timedelta, datetime
from os.path import expanduser, join, abspath

spark = SparkSession.builder.appName('mstr_metrics').getOrCreate()
spark.conf.set("mapred.input.dir.recursive","true")
spark.conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
spark.conf.set("spark.sql.hive.convertMetastoreParquet","false")

status_date = df.withColumn("daysBetween", sf.datediff("maxDate", "minDate"))\
.withColumn("repeat", sf.expr("split(repeat(',', daysBetween), ',')"))\
.select("*", sf.posexplode("repeat").alias("STATUS_DATE", "val"))\
.withColumn("STATUS_DATE", sf.expr("date_add(minDate, STATUS_DATE)"))\
.select('STATUS_DATE')

status_date = status_date.withColumn('YR', sf.year(sf.col('STATUS_DATE')))\
.withColumn('MNTH', sf.from_unixtime(sf.unix_timestamp(sf.col('STATUS_DATE'),'MM/dd/yyyy'), "MMMMM"))\
.withColumn('MNTHYR',sf.concat(sf.col("MNTH"), sf.lit("-"), sf.col("YR")))\
.withColumn('DoW', sf.from_unixtime(sf.unix_timestamp(sf.col('STATUS_DATE'),'MM/dd/yyyy'), 'EEEEE'))\
.withColumn('PoW', sf.when(sf.col('DoW').isin('Saturday','Sunday'), 'WeekEnd').otherwise('WeekDay'))

status_date.show()

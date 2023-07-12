##/**------------------------------------------------------------------------------------------**/
##/**          AMERICAN ELECTRIC POWER - CTIES GIS Meter-Transform Mapping Data                **/
##/**------------------------------------------------------------------------------------------**/
##/**                               Confidentiality Information:                               **/
##/**                               Copyright 2022 by                                          **/
##/**                               American Electric Power                                    **/
##/**                                                                                          **/
##/** This module is confidential and proprietary information of American Electric             **/
##/** Power, it is not to be copied or reproduced in any form, by any means, in                **/
##/** whole or in part, nor is it to be used for any purpose other than that for               **/
##/** which it is expressly provide without written permission of AEP.                         **/
##/**------------------------------------------------------------------------------------------**/
##/** AEP Custom Changes                                                                       **/
##/**  Version #   Name            Date        Description                                     **/
##/**   V0.1      Diganta        11/21/2022  script for cties(raw to stg) glue-etl job         **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/
## s3://aep-datalake-apps-dev/hdpapp/cties/glueetl/cties-meter-xfmr-map-raw-stg.py
##==============================================================================================##
## Import Libraries 

from pyspark.context import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.types import StringType, StructType, StructField, DoubleType
from pyspark.sql.functions import isnull, col, lit, when, to_date, count, array, udf,unix_timestamp


import os
import sys 
from datetime import datetime
import json
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from geopy import distance

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import logging
import time
t = time.time()

import pyarrow as pa
import pyarrow.parquet as pq
import traceback
import pandas as pd

##==============================================================================================##
## Collect Run Parameters 

# spark_appname = "CTies - Load Meter Xfmr Map"

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'AWS_ENV', 'VAR_OPCO', 'VAR_EXTRACT_DT_FROM', 'VAR_EXTRACT_DT_TO' ,'S3_DATA_RAW', 'S3_DATA_WORK','S3_DATA_CONSUME' ,  'VAR_SNS_TOPIC'])

spark_appname = args['JOB_NAME']
AWS_ENV = args['AWS_ENV']
AWS_ENV = AWS_ENV.lower()

VAR_OPCO = args['VAR_OPCO'] ## "pso" 
VAR_OPCO = VAR_OPCO.lower()

S3_DATA_RAW = args['S3_DATA_RAW']
s3_data_raw = S3_DATA_RAW.lower() + "-" + AWS_ENV

S3_DATA_WORK = args['S3_DATA_WORK']
s3_data_work = S3_DATA_WORK.lower() + "-" + AWS_ENV

S3_DATA_CONSUME = args['S3_DATA_CONSUME']

VAR_EXTRACT_DT_FROM = args['VAR_EXTRACT_DT_FROM']
VAR_EXTRACT_DT_TO = args['VAR_EXTRACT_DT_TO']
VAR_EXTRACT_DATES = pd.date_range(start=VAR_EXTRACT_DT_FROM, end=VAR_EXTRACT_DT_TO, freq='D').strftime("%Y%m%d")
# print(VAR_EXTRACT_DATES)

## s3://aep-datalake-raw-dev/util/cties/extract_dt=20221031/aep_opco=<opco>/

VAR_SNS_TOPIC = args['VAR_SNS_TOPIC'] ## ARN of SNS Topic
VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]
print('DEBUG: VAR_SNS_TOPIC_SUCCESS:',VAR_SNS_TOPIC_SUCCESS)
print('DEBUG: VAR_SNS_TOPIC_FAILURE',VAR_SNS_TOPIC_FAILURE)
# spark_appname = VAR_OPCO + " / " + "Load XFMR - Meter Mapping - Raw-To-Stg"

##==============================================================================================##
## CSV Path should get build by the Main Script

xfmr_meter_map_basePath = s3_data_work + "/util/cties/meter_xfmr_mapping_stg/"

##==============================================================================================##
## Setup Spark Config -- Mostly Boilerplate

spark = SparkSession.builder.appName(spark_appname)\
.config("spark.ui.port", "4046")\
.config("spark.sql.broadcastTimeout", "3600")\
.config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")\
.config("hive.exec.dynamic.partition", "true")\
.config("hive.exec.dynamic.partition.mode", "nonstrict")\
.config("spark.sql.adaptive.enabled", "true")\
.config("spark.sql.parquet.mergeSchema", "false")\
.config("spark.sql.files.maxPartitionBytes", 512000000)\
.config("spark.sql.files.maxRecordsPerFile", 50000000)\
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
.config("spark.kryoserializer.buffer.max", "2000m")\
.config("spark.sql.session.timeZone", "EST5EDT")\
.config("spark.sql.debug.maxToStringFields", 1000)\
.config("spark.sql.execution.arrow.pyspark.enabled", "true")\
.config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")\
.enableHiveSupport().getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc, spark)

glueContext = GlueContext(sc.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

conf=SparkConf()

## Various LOG Levels Are ALL, INFO, WARN, ERROR, FATAL, OFF, DEBUG, TRACE

current_user1 = sqlContext._sc.sparkUser()

logger = logging.getLogger()
logger.setLevel(logging.WARN)

## create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.WARN)

## create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

## add the handlers to logger
logger.addHandler(ch)

# get clients
sns_client=boto3.client('sns')
## initiate s3 resource through boto3
s3_resource = boto3.resource('s3') 
s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')
sns_client = boto3.client("sns")
glue_client = boto3.client('glue')
##==============================================================================================##
## Helper Functions

def truncate_stg_table(bucket_name, prefix):
    bucket=s3_resource.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


def file_paths_to_be_processed(bucket_name:str, source_folder_prefix:list) -> list:    
    source_bucket = s3_resource.Bucket(bucket_name)
    # source_folder_prefix = ['util/cties/extract_dt=20221127/aep_opco=pso/','util/cties/extract_dt=20221116/aep_opco=pso/']
    lst_file_names=[]                            
    for pref in source_folder_prefix:
        for obj in (source_bucket.objects.filter(Prefix=pref)):
            lst_file_names.append(pref)
    return list(set(lst_file_names))


def current_timestamp():
    return datetime.today().strftime(' %Y-%m-%d %H:%M:%S.%f ')

def fnLog(severity, logmsg):
    print(current_timestamp() + severity.upper() + logmsg)
    logger.log(getattr(logging, severity), logmsg)
    return

def snsLog(snsTopicARN, msg_subject, msg_body):
    logger.info(current_timestamp() + msg_body)
    response = sns_client.publish(TargetArn=snsTopicARN, Message=json.dumps({'default': json.dumps(msg_body)}), Subject=msg_subject, MessageStructure='json')
    return

@udf(returnType=DoubleType())
def udf_distance_feets(m, t):
    return distance.distance(m,t).feet

# try:
##==============================================================================================##
## Get S3 paths of New files to be ingested

csv_paths = ["util/cties/" + f"extract_dt={dt}/aep_opco={VAR_OPCO}" for dt in VAR_EXTRACT_DATES ]   
# csv_paths = ["util/cties/" + f"extract_dt={dt}" for dt in VAR_EXTRACT_DATES ] 
print("DEBUG: csv_paths before:",csv_paths)
s3_data_raw_bucket_name=s3_data_raw[5:]
lst_file_paths=file_paths_to_be_processed(s3_data_raw_bucket_name, csv_paths)
print('lst_file_paths:',str(lst_file_paths))
# if len(lst_file_paths) == 0:
#     print(f"DEBUG: No New GIS Data for opco - {VAR_OPCO} - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
#     msg_title="Glue ETL: GIS Data Ingestion for opco: "+VAR_OPCO +" " + current_timestamp()
#     msg_body="## Glue ETL: " + spark_appname + " No New GIS Data for opco: "+VAR_OPCO +" " + current_timestamp()
#     snsLog(VAR_SNS_TOPIC_SUCCESS, msg_title, msg_body)
#     job.commit()
#     os._exit(0)

csv_paths=[s3_data_raw+ '/' + x for x in lst_file_paths]
print("DEBUG: data paths to be processed - csv_paths: ",csv_paths)

##==============================================================================================##
## truncate staging table through boto3 by using s3 bucket prefix

s3_data_work_bucket_name=s3_data_work[5:]
staging_data_prefix=f"util/cties/meter_xfmr_mapping_stg/aep_opco={VAR_OPCO}/"  
# staging_data_prefix=f"util/cties/meter_xfmr_mapping_stg/"

truncate_stg_table(s3_data_work_bucket_name, staging_data_prefix)
print(f"DEBUG: truncated stage[meter_xfmr_mapping_stg] table for opco - {VAR_OPCO} - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

##==============================================================================================##
## Define File Structure -- CSV File
## Revised the # of Fields down to following fields.

gis_xfmr_mp_schema=StructType([
StructField('aep_premise_nb',StringType(),True),
StructField('aep_premise_loc_nb',StringType(),True),
StructField('aep_premise_long',StringType(),True),
StructField('aep_premise_lat',StringType(),True),
StructField('trsf_pole_nb',StringType(),True),
StructField('xfmr_long',StringType(),True),
StructField('xfmr_lat',StringType(),True),
StructField('trsf_mount_cd',StringType(),True),
StructField('gis_circuit_nb',StringType(),True),
StructField('gis_circuit_nm',StringType(),True),
StructField('gis_station_nb',StringType(),True),
StructField('gis_station_nm',StringType(),True),
StructField('opco',StringType(),True),
StructField('company_cd',StringType(),True),
StructField('srvc_entn_cd',StringType(),True),
StructField('asof_dt',StringType(),True)
])

##==============================================================================================##
## Read CSV File(s)

gis_xfmr_mp=spark.read.schema(gis_xfmr_mp_schema) \
.option("inferSchema", False) \
.option("mergeSchema", False) \
.option("delimiter", ",") \
.csv(csv_paths)

gis_xfmr_mp_count = gis_xfmr_mp.count()
print("DEBUG: raw files record count - "  + str(gis_xfmr_mp_count))
# if gis_xfmr_mp_count == 0:
#     print(f"DEBUG: Source files are blank for opco - {VAR_OPCO} - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
#     msg_title="Glue ETL: GIS Data Ingestion for opco: "+VAR_OPCO +" " + current_timestamp()
#     msg_body="## Glue ETL: " + spark_appname + " Source files are blank for opco: "+VAR_OPCO +" " + current_timestamp()
#     snsLog(VAR_SNS_TOPIC_SUCCESS, msg_title, msg_body)
#     job.commit()
#     os._exit(0)

##==============================================================================================##
## Derive opco  
gis_xfmr_mp=gis_xfmr_mp.withColumn('aep_opco',\
when(gis_xfmr_mp.opco=='Ohio Power', lit('oh'))\
.when(gis_xfmr_mp.opco=='Columbus Southern Power', lit('oh'))\
.when(gis_xfmr_mp.opco=='Appalachian Power', lit('ap'))\
.when(gis_xfmr_mp.opco=='Wheeling Power', lit('ap'))\
.when(gis_xfmr_mp.opco=='Kingsport Power', lit('ap'))\
.when(gis_xfmr_mp.opco=='AEP Texas North', lit('tx'))\
.when(gis_xfmr_mp.opco=='AEP Texas Central', lit('tx'))\
.when(gis_xfmr_mp.opco=='Indiana Michigan Power', lit('im'))\
.when(gis_xfmr_mp.opco=='Public Service of Oklahoma', lit('pso'))\
.when(gis_xfmr_mp.opco=='Southwestern Electric Power', lit('swp'))\
.when(gis_xfmr_mp.opco=='Kentucky Power', lit('kpc'))\
.otherwise(lit('XX')))

##==============================================================================================##
## Filter out other/invalid opco Data
gis_xfmr_mp=gis_xfmr_mp.filter(gis_xfmr_mp.aep_opco==VAR_OPCO)

print(f"DEBUG: gis_xfmr_mp valid record count with {VAR_OPCO}: "  + str(gis_xfmr_mp.count()))

gis_xfmr_mp=gis_xfmr_mp\
.withColumn('validated_asof_dt', to_date(col('asof_dt'), "yyyy-MM-dd"))

gis_xfmr_mp_counts = gis_xfmr_mp.groupby(gis_xfmr_mp.validated_asof_dt).agg({"*":"count"}).withColumnRenamed('count(1)', 'rowcnt')
gis_xfmr_mp_counts.show(truncate=False)

gis_xfmr_mp=gis_xfmr_mp\
.withColumn('bad_asof_dt', when(col('validated_asof_dt').isNull(), lit(1)).otherwise(0))

gis_xfmr_mp_counts = gis_xfmr_mp.groupby(gis_xfmr_mp.bad_asof_dt).agg({"*":"count"}).withColumnRenamed('count(1)', 'rowcnt')
gis_xfmr_mp_counts.show(truncate=False)

gis_xfmr_mp=gis_xfmr_mp\
.withColumn('validated_asof_dt', when( (gis_xfmr_mp.bad_asof_dt==1), to_date(col('asof_dt'), "yyyy/MM/dd")).otherwise(col('validated_asof_dt')))

gis_xfmr_mp_counts = gis_xfmr_mp.groupby(gis_xfmr_mp.validated_asof_dt).agg({"*":"count"}).withColumnRenamed('count(1)', 'rowcnt')
gis_xfmr_mp_counts.show(truncate=False)

gis_xfmr_mp=gis_xfmr_mp.drop('asof_dt', 'bad_asof_dt')
gis_xfmr_mp=gis_xfmr_mp.withColumnRenamed('validated_asof_dt', 'asof_dt')


gis_xfmr_mp.printSchema()
# print("DEBUG: GIS XFMR-Meter Mapping Data - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
print("DEBUG: GIS XFMR-Meter Mapping Data -  - Partitions = "  + str(gis_xfmr_mp.rdd.getNumPartitions()))
# gis_xfmr_mp.show(truncate=False)

##==============================================================================================##
## Audit Check Display
gis_xfmr_mp_counts = gis_xfmr_mp.groupby(gis_xfmr_mp.aep_opco).agg({"*":"count"}).withColumnRenamed('count(1)', 'rowcnt')
# gis_xfmr_mp_counts.show(truncate=False)

gis_xfmr_mp=gis_xfmr_mp.drop('opco_name')


gis_xfmr_mp = gis_xfmr_mp \
.withColumn('calc_aep_premise_long', gis_xfmr_mp.aep_premise_long.cast("double")) \
.withColumn('calc_aep_premise_lat', gis_xfmr_mp.aep_premise_lat.cast("double")) \
.withColumn('calc_xfmr_long', gis_xfmr_mp.xfmr_long.cast("double")) \
.withColumn('calc_xfmr_lat', gis_xfmr_mp.xfmr_lat.cast("double"))

gis_xfmr_mp = gis_xfmr_mp \
.withColumn('aep_premise_coord', array(gis_xfmr_mp.calc_aep_premise_lat, gis_xfmr_mp.calc_aep_premise_long))\
.withColumn('xfmr_coord', array(gis_xfmr_mp.calc_xfmr_lat, gis_xfmr_mp.calc_xfmr_long))

## Create a new column 'distance_feets'
gis_xfmr_mp = gis_xfmr_mp \
.withColumn('distance_feets', udf_distance_feets(gis_xfmr_mp.aep_premise_coord, gis_xfmr_mp.xfmr_coord))

gis_xfmr_mp=gis_xfmr_mp.drop('calc_aep_premise_long', 'calc_aep_premise_lat', 'calc_xfmr_long', 'calc_xfmr_lat', 'aep_premise_coord', 'xfmr_coord')
gis_xfmr_mp.printSchema()

##==============================================================================================##
## Create a new columns 'aws_update_dttm' and 'run_control_id' and select final dataset

gis_xfmr_mp = gis_xfmr_mp\
.withColumn('aws_update_dttm', lit(datetime.today().strftime("%Y-%m-%d %H:%M:%S")))

gis_xfmr_mp = gis_xfmr_mp\
.withColumn('run_control_id', unix_timestamp(gis_xfmr_mp.aws_update_dttm , 'yyyy-MM-dd HH:mm:ss'))

gis_xfmr_mp = gis_xfmr_mp\
.select('aep_premise_nb','aep_premise_loc_nb','aep_premise_long','aep_premise_lat', 'trsf_pole_nb','xfmr_long','xfmr_lat','trsf_mount_cd', 'gis_circuit_nb','gis_circuit_nm','gis_station_nb','gis_station_nm','company_cd','srvc_entn_cd', 'distance_feets','aws_update_dttm','run_control_id','aep_opco','asof_dt')

##==============================================================================================##
## Repartition the Data

gis_xfmr_mp=gis_xfmr_mp.repartition("aep_opco", "asof_dt") 
print("DEBUG: Done with Repartition the DF - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
##==============================================================================================##
## Write the Data Out to our Target Folder

print("DEBUG: Starting To Write the Frame to XFMR Mapping Folder - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

gis_xfmr_mp.write.mode("overwrite") \
.partitionBy("aep_opco", "asof_dt") \
.option("parquet.bloom.filter.enabled#aep_premise_nb", "true")\
.option("parquet.bloom.filter.enabled#trsf_pole_nb", "true")\
.option("parquet.bloom.filter.expected.ndv#aep_premise_nb", "5000000")\
.option("parquet.bloom.filter.expected.ndv#trsf_pole_nb", "2000000")\
.option("compression", "SNAPPY") \
.option("spark.sql.files.maxRecordsPerFile", 1000000) \
.format("parquet") \
.save(xfmr_meter_map_basePath)

print("DEBUG: Done with Writing to XFMR Mapping Folder - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

##==============================================================================================##
## Will need MSCK after Spark is done.
print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table stg_cties.meter_xfmr_mapping_stg")

fnLog("INFO", " Partition " + xfmr_meter_map_basePath + " successfully loaded.")
fnLog("INFO", " Load succeeded in " + str(round(time.time() - t,2)) + " seconds.")

##==============================================================================================##
## Stop the Context
job.commit()
sc.stop()

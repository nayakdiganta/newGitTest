# /**------------------------------------------------------------------------------------------**/
# /**    AMERICAN ELECTRIC POWER - Meter Event Outage Summary     GlueETL                      **/
# /**------------------------------------------------------------------------------------------**/
# /**                               Confidentiality Information:                               **/
# /**                               Copyright 2022, 2023 by                                    **/
# /**                               American Electric Power                                    **/
# /**                                                                                          **/
# /** This module is confidential and proprietary information of American Electric             **/
# /** Power, it is not to be copied or reproduced in any form, by any means, in                **/
# /** whole or in part, nor is it to be used for any purpose other than that for               **/
# /** which it is expressly provide without written permission of AEP.                         **/
# /**--------------------------------------------------------end_device_event------------------**/
# /** AEP Custom Changes                            all                                        **/
# /**  Version #   Name                     Date            Description                        **/
# /**   V0.1       Diganta                  11/14/2022      First Attempt in CloudFormation    **/
# /**                                                                                          **/
# /**------------------------------------------------------------------------------------------**/

from pyspark.context import SparkContext, SparkConf

# from pyspark.sql import HiveContext
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    ArrayType,
    IntegerType,
    LongType
)
from pyspark.sql import functions as F, Window as W
from pyspark.sql.functions import collect_list
import sys
import os

import re
from datetime import datetime  # , date, timedelta
import json
# from pytz import timezone
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# import base64
import logging

# import uuid
from time import time
from awsglue.dynamicframe import DynamicFrame
t = time()


# get clients
ssm_client = boto3.client('ssm')
sns_client = boto3.client("sns")
glue_client = boto3.client('glue')


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

##==============================================================================================##
## Helper Functions

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

## ==============================================================================================##
# Collect Run Parameters
# args = getResolvedOptions( sys.argv, ["JOB_NAME", "AWS_ENV", "VAR_OPCO", "VAR_AEP_EVENT_DT_FROM", "VAR_AEP_EVENT_DT_TO", "S3_METER_EVENT_CONSUME", "VAR_SNS_TOPIC","S3_LANDING_PATH", "METER_PREM_DB"])
args = getResolvedOptions( sys.argv, ["JOB_NAME", "AWS_ENV", "VAR_OPCO", "VAR_AEP_EVENT_DT_FROM", "VAR_AEP_EVENT_DT_TO", "S3_DATA_WORK", "S3_DATA_CONSUME", "VAR_SNS_TOPIC"])

SPARK_APPNAME = args["JOB_NAME"]
AWS_ENV = args["AWS_ENV"]
AWS_ENV = AWS_ENV.lower()

VAR_OPCO = args["VAR_OPCO"]  # "pso"
VAR_OPCO = VAR_OPCO.lower()

S3_DATA_WORK = args["S3_DATA_WORK"] 
S3_DATA_WORK = S3_DATA_WORK.lower() + "-" + AWS_ENV

S3_DATA_CONSUME = args["S3_DATA_CONSUME"] 
S3_DATA_CONSUME = S3_DATA_CONSUME.lower() + "-" + AWS_ENV
print('S3_DATA_CONSUME:',S3_DATA_CONSUME)

VAR_SNS_TOPIC = args["VAR_SNS_TOPIC"]  # ARN of SNS Topic
VAR_AEP_EVENT_DT_FROM = args["VAR_AEP_EVENT_DT_FROM"]
VAR_AEP_EVENT_DT_TO = args["VAR_AEP_EVENT_DT_TO"]

event_summ_regex_setup_path = S3_DATA_CONSUME + "/util/event_summ_regex_setup"

end_device_event_catg_basePath=S3_DATA_WORK + "/util/events/uiq/end_device_event_catg_stg"  
events_summary_consume_basePath  =  S3_DATA_CONSUME + "/util/events_summary"
end_device_event_errors_basePath = S3_DATA_WORK + "/util/events/uiq/end_device_event_errors"

##==============================================================================================##
## Setup Spark Config -- Mostly Boilerplate

spark = SparkSession.builder.appName(SPARK_APPNAME)\
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

VAR_AEP_EVENT_DATES = pd.date_range(start=VAR_AEP_EVENT_DT_FROM, end=VAR_AEP_EVENT_DT_TO, freq="D").strftime("%Y-%m-%d")
# end_device_event_catg_stg_basePath
end_device_event_catg_paths = []
for event_dt in VAR_AEP_EVENT_DATES:
    end_device_event_catg_paths = end_device_event_catg_paths + [ os.path.join( end_device_event_catg_basePath,f"aep_opco={VAR_OPCO}",f"aep_event_dt={event_dt}") ]


print('end_device_event_catg_paths:',str(end_device_event_catg_paths) )
##==============================================================================================##
## Define File Structure -- end_device_event_catg

end_device_event_catg_schema=StructType([
StructField("issuertracking_id", StringType(), True),
StructField("serialnumber", StringType(), True),
StructField("enddeviceeventtypeid", StringType(), True),
StructField("valuesinterval", StringType(), True),
StructField("aep_devicecode", StringType(), True),
StructField("aep_mtr_pnt_nb", StringType(), True),
StructField("aep_tarf_pnt_nb", StringType(), True),
StructField("aep_premise_nb", StringType(), True),
StructField("aep_state", StringType(), True),
StructField("aep_area_cd", StringType(), True),
StructField("aep_sub_area_cd", StringType(), True),
StructField("longitude", StringType(), True),
StructField("latitude", StringType(), True),
StructField("aep_city", StringType(), True),
StructField("aep_zip", StringType(), True),
StructField("trsf_pole_nb", StringType(), True),
StructField("circuit_nb", StringType(), True),
StructField("circuit_nm", StringType(), True),
StructField("station_nb", StringType(), True),
StructField("station_nm", StringType(), True),
StructField("xf_meter_cnt", LongType(), True),
StructField("reason", StringType(), True),
StructField("curated_reason", StringType(), True),
StructField("regex_id", StringType(), True),
StructField("aws_update_dttm", StringType(), True),
StructField("aep_opco", StringType(), True),
StructField("aep_event_dt", StringType(), True)
])

end_device_event_catg_df =  spark.read.schema(end_device_event_catg_schema) \
.option("basePath", end_device_event_catg_basePath) \
.option("inferSchema", "false") \
.option("compression", "snappy") \
.format("parquet") \
.load(path=end_device_event_catg_paths)


##==============================================================================================##
## Retrieve Regex details from regex_setup table and convert to dict

regex_setup_schema=StructType([
StructField("regex_id",StringType(),True),
StructField("enddeviceeventtypeid",StringType(),True),
StructField("regex_seq_no",StringType(),True),
StructField("regex_search_pattern",StringType(),True),
StructField("regex_replacement_pattern",StringType(),True),
StructField("pivot_group",LongType(),True),
StructField("pivot_id",StringType(),True),
StructField("printing_ord_nb",LongType(),True),
StructField("regex_status",StringType(),True),
StructField("regex_descr",StringType(),True),
StructField("regex_report_title",StringType(),True),
StructField("regex_category",StringType(),True),
StructField("itron_event_id",StringType(),True),
StructField("itron_event_desc",StringType(),True),
StructField("aws_update_dttm",StringType(),True)
])

regex_setup_df =  spark.read.schema(regex_setup_schema) \
.option("inferSchema", "false") \
.option("mergeSchema", "false") \
.option("delimiter", "~") \
.format("csv") \
.load(path=event_summ_regex_setup_path)
# regex_setup_df.show(4)


##==============================================================================================##
## Join with regex_set_up table to get pivot_id based on regex_id and regex_report_title/curated_reason

regex_setup_df = regex_setup_df.withColumnRenamed('regex_id','r_regex_id') \
    .withColumnRenamed('enddeviceeventtypeid','r_enddeviceeventtypeid') \
    .withColumnRenamed('aws_update_dttm','r_aws_update_dttm') 

join_cond=(end_device_event_catg_df.regex_id == regex_setup_df.r_regex_id) & (F.trim(end_device_event_catg_df.curated_reason) == F.trim(regex_setup_df.regex_report_title))

event_df = end_device_event_catg_df.join(regex_setup_df,join_cond, how="left_outer") 
event_df.cache()
##==============================================================================================##
## Join with regex_set_up table to get pivot_id based on regex_id and regex_report_title/curated_reason
## move the records to error table, if having new curated_reason/regex_report_title

error_events_df=event_df.filter(F.col("regex_report_title").isNull()) \
    .withColumn("hist_or_incr", F.lit('events_summary') ) \
    .withColumn("error_descr", F.concat_ws('|', F.lit("new regex_report_title"),event_df.curated_reason ) ) \
    .withColumn("run_dt", F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss") )

error_events_df = error_events_df.select( \
    F.col('issuertracking_id') , \
    F.lit('').alias('issuer_id'), \
    F.col('serialnumber'), \
    F.col('enddeviceeventtypeid'), \
    F.lit('').alias('aep_timezone_cd'), \
    F.col('valuesinterval'), \
    F.col('aep_devicecode'), \
    F.col('aep_mtr_pnt_nb'), \
    F.col('aep_tarf_pnt_nb'), \
    F.col('aep_premise_nb'), \
    F.lit('').alias('aep_service_point'), \
    F.lit('').alias('aep_bill_account_nb'), \
    F.col('reason'), \
    F.lit('').alias('user_id'), \
    F.lit('').alias('manufacturer_id'), \
    F.lit('').alias('domain'), \
    F.lit('').alias('eventoraction'), \
    F.lit('').alias('sub_domain'), \
    F.lit('').alias('event_type'), \
    F.col('aep_state'), \
    F.col('aep_area_cd'), \
    F.col('aep_sub_area_cd'), \
    F.col('longitude'), \
    F.col('latitude'), \
    F.col('aep_city'), \
    F.col('aep_zip'), \
    F.lit('').alias('hdp_update_user'), \
    F.lit(F.current_timestamp()).alias('hdp_insert_dttm'), \
    F.lit(F.current_timestamp()).alias('hdp_update_dttm'), \
    F.col('hist_or_incr'), \
    F.col('error_descr'), \
    F.col('run_dt'), \
    F.col('aep_opco'), \
    F.col('aep_event_dt') \
 )

print('DEBUG: error_events_df printSchema')
error_events_df.printSchema()
error_events_df = error_events_df.repartition("run_dt","aep_opco", "aep_event_dt") 
# error_events_df.show(10,truncate = False)
error_events_df.write.mode("append") \
.partitionBy("run_dt","aep_opco", "aep_event_dt") \
.format("orc") \
.option("compression", "SNAPPY") \
.save(end_device_event_errors_basePath)

##==============================================================================================##
## MSCK
print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table stg_meterevents.end_device_event_errors")

##==============================================================================================##
## process/insert the records having curated_reason/regex_report_title to events_summary table

event_df=event_df.filter(F.col("regex_report_title").isNotNull()) 

event_df=event_df.withColumn('kvmap',F.create_map(event_df.pivot_id,F.lit(1))) 
# event_df.show(5, truncate=-False)

event_df=event_df.withColumn('aws_update_dttm', F.lit(datetime.today().strftime("%Y-%m-%d %H:%M:%S")))

static_cols=['issuertracking_id' ,'serialnumber','aep_premise_nb','trsf_pole_nb','xf_meter_cnt','enddeviceeventtypeid','valuesinterval','reason','curated_reason','regex_id','pivot_id']

lst_pivot_id_col_values = [row.pivot_id for row in regex_setup_df.select('pivot_id').collect() ] ##['cr1','cr2']
dynamic_cols=[f"COALESCE(kvmap['{i}'], 0) AS {i}" for i in lst_pivot_id_col_values]

aws_update_dttm_col =['aws_update_dttm']

partition_cols=['aep_opco','aep_event_dt']

select_expr=static_cols + dynamic_cols + aws_update_dttm_col + partition_cols
print('select_expr:', str(select_expr))

event_df=event_df.selectExpr(*select_expr)
# event_df.show(5, truncate=-False)

print('event_count_df count:', str(event_df.count()))
event_df=event_df.na.fill(0)

es_final = event_df.repartition('aep_opco', 'aep_event_dt')
es_final.printSchema()
# es_final.show(5, truncate=False)
es_final.write.mode("overwrite") \
.partitionBy("aep_opco", "aep_event_dt") \
.option("parquet.bloom.filter.enabled#aep_premise_nb", "true")\
.option("parquet.bloom.filter.enabled#serialnumber", "true")\
.option("parquet.bloom.filter.enabled#trsf_pole_nb", "true")\
.option("parquet.bloom.filter.expected.ndv#aep_premise_nb", "5000000")\
.option("parquet.bloom.filter.expected.ndv#serialnumber", "5000000")\
.option("parquet.bloom.filter.expected.ndv#trsf_pole_nb", "2000000")\
.option("parquet.enable.dictionary", "true")\
.format("parquet") \
.option("compression", "SNAPPY") \
.option("spark.sql.files.maxRecordsPerFile", 250000) \
.save(events_summary_consume_basePath)

fnLog("INFO", " DEBUG: Done with Writing out the DF  - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
fnLog("INFO", " Partition " + events_summary_consume_basePath + " successfully loaded.")

##==============================================================================================##
## MSCK
print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table meter_events.events_summary") 

fnLog("INFO", " meter_events.events_summary successfully loaded.")

###################
# ## view creation

# static_cols_vw=['serialnumber','aep_premise_nb','trsf_pole_nb','xf_meter_cnt','aep_opco','aep_event_dt']
# dynamic_cols_vw=[f"sum(COALESCE({i}, 0)) AS {i}" for i in lst_pivot_id_col_values]
# groupby_cols_vw=['serialnumber','aep_premise_nb','trsf_pole_nb','xf_meter_cnt','aep_opco','aep_event_dt']
# groupby_cols_vw = ','.join(groupby_cols_vw)
# select_cols_vw=static_cols_vw + dynamic_cols_vw 
# select_cols_vw = ','.join(select_cols_vw)
# view_nm = 'meter_events.events_summary_vw'
# view_location = "s3://aep-datalake-consume-dev/util/events_summary_vw"
# create_vw_str = f"create or replace view {view_nm} LOCATION '{view_location}' AS  select  {select_cols_vw} from meter_events.events_summary group by {groupby_cols_vw}"
# print(create_vw_str)
# spark.sql(create_vw_str) 

## Stop the Context
job.commit()
sc.stop()

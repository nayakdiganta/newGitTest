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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
s3_client = boto3.client('s3')


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
args = getResolvedOptions( sys.argv, ["JOB_NAME", "AWS_ENV", "S3_DATA_WORK", "S3_DATA_CONSUME", "VAR_SNS_TOPIC"])

SPARK_APPNAME = args["JOB_NAME"]
AWS_ENV = args["AWS_ENV"]
AWS_ENV = AWS_ENV.lower()

S3_DATA_WORK = args["S3_DATA_WORK"] 
S3_DATA_WORK = S3_DATA_WORK.lower() + "-" + AWS_ENV

S3_DATA_CONSUME = args["S3_DATA_CONSUME"] 
S3_DATA_CONSUME = S3_DATA_CONSUME.lower() + "-" + AWS_ENV
print('S3_DATA_CONSUME:',S3_DATA_CONSUME)

VAR_SNS_TOPIC = args["VAR_SNS_TOPIC"]  # ARN of SNS Topic
# VAR_AEP_EVENT_DT_FROM = args["VAR_AEP_EVENT_DT_FROM"]
# VAR_AEP_EVENT_DT_TO = args["VAR_AEP_EVENT_DT_TO"]

event_summ_regex_setup_path = S3_DATA_CONSUME + "/util/event_summ_regex_setup"

end_device_event_catg_basePath=S3_DATA_WORK + "/util/events/uiq/end_device_event_catg_stg"  
events_summary_consume_basePath  =  S3_DATA_CONSUME + "/util/events_summary"
end_device_event_errors_basePath = S3_DATA_WORK + "/util/events/uiq/end_device_event_errors"

##==============================================================================================##
##### retrieve outg_summ_opco_eventdt_dict value from ssm parameter

ssm_outg_summ_opco_event_dt_dict_name="/aep/analytics/hdp/" + AWS_ENV + "/ppo/parms/outg_summ_opco_event_dt_dict"
ssm_outg_summ_opco_event_dt_dict_resp = ssm_client.get_parameter(Name=ssm_outg_summ_opco_event_dt_dict_name, WithDecryption=False)
ssm_outg_summ_opco_event_dt_dict_val = ssm_outg_summ_opco_event_dt_dict_resp['Parameter']['Value']

print('DEBUG: ssm_outg_summ_opco_event_dt_dict_val=',ssm_outg_summ_opco_event_dt_dict_val)

if ssm_outg_summ_opco_event_dt_dict_val == '{}': 
    print('INFO: Exiting the job process as it could not find any new data/files to process..')
    os._exit(0)

## Read it back as a dict
dict_opco_event_dt={}
dict_opco_event_dt = json.loads(ssm_outg_summ_opco_event_dt_dict_val)

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

##==============================================================================================##
## Get only the incremental data from _catg based on dict_opco_event_dt

end_device_event_catg_paths = [] 
for opco,event_dt_lst in dict_opco_event_dt.items():
    for event_dt in set(event_dt_lst):
        end_device_event_catg_paths = end_device_event_catg_paths + [ os.path.join( end_device_event_catg_basePath,f"aep_opco={opco}",f"aep_event_dt={event_dt}") ]


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

##==============================================================================================##
## create end_device_event_catg_df

end_device_event_catg_df =  spark.read.schema(end_device_event_catg_schema) \
.option("basePath", end_device_event_catg_basePath) \
.option("inferSchema", "false") \
.option("compression", "snappy") \
.format("parquet") \
.load(path=end_device_event_catg_paths)

##==============================================================================================##
## Get only the incremental data from events_summary based on dict_opco_event_dt

events_summary_paths = [] 
for opco,event_dt_lst in dict_opco_event_dt.items():
    for event_dt in set(event_dt_lst):
        events_summary_paths = events_summary_paths + [ os.path.join( events_summary_consume_basePath,f"aep_opco={opco}",f"aep_event_dt={event_dt}") ]

##==============================================================================================##
## check if all s3 events_summary_paths paths are valid

s3_data_consume_bucket_name=S3_DATA_CONSUME[5:] 
all_paths = events_summary_paths
events_summary_paths = []
for path in all_paths:
    bucket_name, key = path.replace('s3://','').split('/',1)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key)
        if 'Contents' in response:
            events_summary_paths.append(path)
    except Exception as e:
        pass
print('events_summary_paths',str(events_summary_paths))

##==============================================================================================##
## Define File Structure -- events_summary

events_summary_schema=StructType([
  StructField("issuertracking_id", StringType(), True), 
  StructField("serialnumber", StringType(), True), 
  StructField("aep_premise_nb", StringType(), True), 
  StructField("trsf_pole_nb", StringType(), True), 
  StructField("xf_meter_cnt", LongType(), True), 
  StructField("enddeviceeventtypeid", StringType(), True), 
  StructField("valuesinterval", StringType(), True), 
  StructField("reason", StringType(), True), 
  StructField("curated_reason", StringType(), True), 
  StructField("regex_id", StringType(), True), 
  StructField("pivot_id", StringType(), True), 
  StructField("cr1",  LongType(), True),
  StructField("cr2",  LongType(), True),
  StructField("cr3",  LongType(), True),
  StructField("cr4",  LongType(), True),
  StructField("cr5",  LongType(), True),
  StructField("cr6",  LongType(), True),
  StructField("cr7",  LongType(), True),
  StructField("cr8",  LongType(), True),
  StructField("cr9",  LongType(), True),
  StructField("cr10", LongType(), True), 
  StructField("cr11", LongType(), True),
  StructField("cr12", LongType(), True),
  StructField("cr13", LongType(), True),
  StructField("cr14", LongType(), True),
  StructField("cr15", LongType(), True),
  StructField("cr16", LongType(), True),
  StructField("cr17", LongType(), True),
  StructField("cr18", LongType(), True),
  StructField("cr19", LongType(), True),
  StructField("cr20", LongType(), True),
  StructField("cr21", LongType(), True),
  StructField("cr22", LongType(), True),
  StructField("cr23", LongType(), True),
  StructField("cr24", LongType(), True),
  StructField("cr25", LongType(), True),
  StructField("cr26", LongType(), True),
  StructField("cr27", LongType(), True),
  StructField("cr28", LongType(), True),
  StructField("cr29", LongType(), True),
  StructField("cr30", LongType(), True),
  StructField("cr31", LongType(), True),
  StructField("cr32", LongType(), True),
  StructField("cr33", LongType(), True),
  StructField("cr34", LongType(), True),
  StructField("cr35", LongType(), True),
  StructField("cr36", LongType(), True),
  StructField("cr37", LongType(), True),
  StructField("cr38", LongType(), True),
  StructField("cr39", LongType(), True),
  StructField("cr40", LongType(), True),
  StructField("cr41", LongType(), True),
  StructField("cr42", LongType(), True),
  StructField("cr43", LongType(), True),
  StructField("cr44", LongType(), True),
  StructField("cr45", LongType(), True),
  StructField("cr46", LongType(), True),
  StructField("cr47", LongType(), True),
  StructField("cr48", LongType(), True),
  StructField("cr49", LongType(), True),
  StructField("cr50", LongType(), True),
  StructField("cr51", LongType(), True),
  StructField("cr52", LongType(), True),
  StructField("cr53", LongType(), True),
  StructField("cr54", LongType(), True),
  StructField("cr55", LongType(), True),
  StructField("cr56", LongType(), True),
  StructField("cr57", LongType(), True),
  StructField("cr58", LongType(), True),
  StructField("cr59", LongType(), True),
  StructField("cr60", LongType(), True),
  StructField("cr61", LongType(), True),
  StructField("cr62", LongType(), True),
  StructField("cr63", LongType(), True),
  StructField("cr64", LongType(), True),
  StructField("cr65", LongType(), True),
  StructField("cr66", LongType(), True),
  StructField("cr67", LongType(), True),
  StructField("cr68", LongType(), True),
  StructField("cr69", LongType(), True),
  StructField("cr70", LongType(), True),
  StructField("cr71", LongType(), True),
  StructField("cr72", LongType(), True),
  StructField("cr73", LongType(), True),
  StructField("cr74", LongType(), True),
  StructField("cr75", LongType(), True),
  StructField("cr76", LongType(), True),
  StructField("cr77", LongType(), True),
  StructField("cr78", LongType(), True),
  StructField("cr79", LongType(), True),
  StructField("cr80", LongType(), True),
  StructField("cr81", LongType(), True),
  StructField("cr82", LongType(), True),
  StructField("cr83", LongType(), True),
  StructField("cr84", LongType(), True),
  StructField("cr85", LongType(), True),
  StructField("cr86", LongType(), True),
  StructField("cr87", LongType(), True),
  StructField("cr88", LongType(), True),
  StructField("cr89", LongType(), True),
  StructField("cr90", LongType(), True),
  StructField("cr91", LongType(), True),
  StructField("cr92", LongType(), True),
  StructField("cr93", LongType(), True),
  StructField("cr94", LongType(), True),
  StructField("cr95", LongType(), True),
  StructField("cr96", LongType(), True),
  StructField("cr97", LongType(), True),
  StructField("cr98", LongType(), True),
  StructField("cr99", LongType(), True),
  StructField("cr100", LongType(), True), 
  StructField("cr101", LongType(), True), 
  StructField("cr102", LongType(), True), 
  StructField("cr103", LongType(), True), 
  StructField("cr104", LongType(), True), 
  StructField("cr105", LongType(), True), 
  StructField("cr106", LongType(), True), 
  StructField("cr107", LongType(), True), 
  StructField("cr108", LongType(), True), 
  StructField("cr109", LongType(), True), 
  StructField("cr110", LongType(), True), 
  StructField("cr111", LongType(), True), 
  StructField("cr112", LongType(), True), 
  StructField("cr113", LongType(), True), 
  StructField("cr114", LongType(), True), 
  StructField("cr115", LongType(), True), 
  StructField("cr116", LongType(), True), 
  StructField("cr117", LongType(), True), 
  StructField("cr118", LongType(), True), 
  StructField("cr119", LongType(), True), 
  StructField("cr120", LongType(), True), 
  StructField("cr121", LongType(), True), 
  StructField("cr122", LongType(), True), 
  StructField("cr123", LongType(), True), 
  StructField("cr124", LongType(), True), 
  StructField("cr125", LongType(), True), 
  StructField("cr126", LongType(), True), 
  StructField("cr127", LongType(), True), 
  StructField("cr128", LongType(), True), 
  StructField("cr129", LongType(), True), 
  StructField("cr130", LongType(), True), 
  StructField("cr131", LongType(), True), 
  StructField("cr132", LongType(), True), 
  StructField("cr133", LongType(), True), 
  StructField("cr134", LongType(), True), 
  StructField("cr135", LongType(), True), 
  StructField("cr136", LongType(), True), 
  StructField("cr137", LongType(), True), 
  StructField("cr138", LongType(), True), 
  StructField("cr139", LongType(), True), 
  StructField("cr140", LongType(), True), 
  StructField("cr141", LongType(), True), 
  StructField("cr142", LongType(), True), 
  StructField("cr143", LongType(), True), 
  StructField("cr144", LongType(), True), 
  StructField("cr145", LongType(), True), 
  StructField("cr146", LongType(), True), 
  StructField("cr147", LongType(), True), 
  StructField("cr148", LongType(), True), 
  StructField("cr149", LongType(), True), 
  StructField("cr150", LongType(), True), 
  StructField("cr151", LongType(), True), 
  StructField("cr152", LongType(), True), 
  StructField("cr153", LongType(), True), 
  StructField("cr154", LongType(), True), 
  StructField("cr155", LongType(), True), 
  StructField("cr156", LongType(), True), 
  StructField("cr157", LongType(), True), 
  StructField("cr158", LongType(), True), 
  StructField("cr159", LongType(), True), 
  StructField("cr160", LongType(), True), 
  StructField("cr161", LongType(), True), 
  StructField("cr162", LongType(), True), 
  StructField("cr163", LongType(), True), 
  StructField("cr164", LongType(), True), 
  StructField("cr165", LongType(), True), 
  StructField("cr166", LongType(), True), 
  StructField("cr167", LongType(), True), 
  StructField("cr168", LongType(), True), 
  StructField("cr169", LongType(), True), 
  StructField("cr170", LongType(), True), 
  StructField("cr171", LongType(), True), 
  StructField("cr172", LongType(), True), 
  StructField("cr173", LongType(), True), 
  StructField("cr174", LongType(), True), 
  StructField("cr175", LongType(), True), 
  StructField("cr176", LongType(), True), 
  StructField("cr177", LongType(), True), 
  StructField("cr178", LongType(), True), 
  StructField("cr179", LongType(), True), 
  StructField("cr180", LongType(), True), 
  StructField("cr181", LongType(), True), 
  StructField("cr182", LongType(), True), 
  StructField("cr183", LongType(), True), 
  StructField("cr184", LongType(), True), 
  StructField("cr185", LongType(), True), 
  StructField("cr186", LongType(), True), 
  StructField("cr187", LongType(), True), 
  StructField("cr188", LongType(), True), 
  StructField("cr189", LongType(), True), 
  StructField("cr190", LongType(), True), 
  StructField("cr191", LongType(), True), 
  StructField("cr192", LongType(), True), 
  StructField("cr193", LongType(), True), 
  StructField("cr194", LongType(), True), 
  StructField("cr195", LongType(), True), 
  StructField("cr196", LongType(), True), 
  StructField("cr197", LongType(), True), 
  StructField("cr198", LongType(), True), 
  StructField("cr199", LongType(), True), 
  StructField("aws_update_dttm", StringType(), True), 
  StructField("aep_opco", StringType(), True),
  StructField("aep_event_dt", StringType(), True)
])

##==============================================================================================##
## create events_summary_df

events_summary_df =  spark.read.schema(events_summary_schema) \
.option("basePath", events_summary_consume_basePath) \
.option("inferSchema", "false") \
.option("compression", "snappy") \
.format("parquet") \
.load(path=events_summary_paths)

events_summary_df = events_summary_df.select( \
    F.col('issuertracking_id') , \
    F.col('serialnumber'), \
    F.col('enddeviceeventtypeid'), \
    F.col('valuesinterval'), \
    F.lit('').alias('aep_devicecode'), \
    F.lit('').alias('aep_mtr_pnt_nb'), \
    F.lit('').alias('aep_tarf_pnt_nb'), \
    F.col('aep_premise_nb'), \
    F.lit('').alias('aep_state'), \
    F.lit('').alias('aep_area_cd'), \
    F.lit('').alias('aep_sub_area_cd'), \
    F.lit('').alias('longitude'), \
    F.lit('').alias('latitude'), \
    F.lit('').alias('aep_city'), \
    F.lit('').alias('aep_zip'), \
    F.col('trsf_pole_nb'), \
    F.lit('').alias('circuit_nb'), \
    F.lit('').alias('circuit_nm'), \
    F.lit('').alias('station_nb'), \
    F.lit('').alias('station_nm'), \
    F.col('xf_meter_cnt'), \
    F.col('reason'), \
    F.col('curated_reason'), \
    F.col('regex_id'), \
    F.lit('').alias('aws_update_dttm'), \
    F.col('aep_opco'), \
    F.col('aep_event_dt') \
 ) 

##==============================================================================================##
## Get unique records from catg and events_summ tables

end_device_event_catg_df=end_device_event_catg_df.union(events_summary_df)
end_device_event_catg_df.printSchema()
windowSpec = Window.partitionBy('issuertracking_id','serialnumber','valuesinterval','enddeviceeventtypeid').orderBy(F.col('aws_update_dttm').desc())
end_device_event_catg_df = end_device_event_catg_df.withColumn('rownum',F.row_number().over(windowSpec) )
end_device_event_catg_df=end_device_event_catg_df.filter(F.col('rownum') == 1 )

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

##==============================================================================================##
## if everything is ok, 
## get parameter value for outage summary data ingestion

ssm_last_run_dt_pass_to_outg_summ_name="/aep/analytics/hdp/"+AWS_ENV+"/ppo/parms/last_run_dt_incr_pass_to_outg_summ"
ssm_last_run_dt_pass_to_outg_summ_resp = ssm_client.get_parameter(Name=ssm_last_run_dt_pass_to_outg_summ_name, WithDecryption=False )
ssm_last_run_dt_pass_to_outg_summ_val = ssm_last_run_dt_pass_to_outg_summ_resp['Parameter']['Value']

run_dt_incr_for_next_run = ssm_last_run_dt_pass_to_outg_summ_val

##  set ssm_last_run_dt_incr value to be passed to ssm_last_run_dt_pass_to_outg_summ
ssm_last_run_dt_incr_name="/aep/analytics/hdp/"+AWS_ENV+"/ppo/parms/last_run_dt_incr"
ssm_last_run_dt_incr_resp = ssm_client.put_parameter(Name=ssm_last_run_dt_incr_name, Value= run_dt_incr_for_next_run , Type='String', Overwrite=True )


## Stop the Context
job.commit()
sc.stop()

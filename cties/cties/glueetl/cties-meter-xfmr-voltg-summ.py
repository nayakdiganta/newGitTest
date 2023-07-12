##/**------------------------------------------------------------------------------------------**/
##/**    AMERICAN ELECTRIC POWER - Customer Ties Meter Transformer Voltage Summary GlueETL     **/
##/**------------------------------------------------------------------------------------------**/
##/**                               Confidentiality Information:                               **/
##/**                               Copyright 2022, 2023 by                                    **/
##/**                               American Electric Power                                    **/
##/**                                                                                          **/
##/** This module is confidential and proprietary information of American Electric             **/
##/** Power, it is not to be copied or reproduced in any form, by any means, in                **/
##/** whole or in part, nor is it to be used for any purpose other than that for               **/
##/** which it is expressly provide without written permission of AEP.                         **/
##/**------------------------------------------------------------------------------------------**/
##/** AEP Custom Changes                                                                       **/
##/**  Version #   Name                     Date            Description                        **/
##/**   V0.1       Diganta                  11/14/2022      First Attempt in CloudFormation    **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/

## Import Libraries -- Mostly Boilerplate
## These Libraries would change based on AWS Glue ETL Setup - Be Minimalistic

from pyspark.context import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, StructType, DecimalType, StructField, DoubleType, FloatType, BinaryType,LongType
from pyspark.sql.types import *
from pyspark.sql.functions import isnull, col, lit, rank, row_number, date_format, desc, concat, concat_ws, substring, to_timestamp, hour, when, first, count, sum, unix_timestamp, round
from pyspark.sql.functions import avg, stddev, mean, percentile_approx
import pyspark.sql.functions as F

import sys
import os
import re
from datetime import datetime, date, timedelta
# import datetime
from pytz import timezone
import json
import boto3
from botocore.exceptions import ClientError
from io import BytesIO, StringIO

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import base64
import logging
import uuid
from time import time
t = time()

import awswrangler as wr
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from pyspark.sql.window import Window

from math import sqrt
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT
import traceback
## from pyspark.ml.feature import MinMaxScaler
## We might need to install scipy
## from scipy.spatial.distance import euclidean

##==============================================================================================##
## Collect Run Parameters 
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'AWS_ENV', 'VAR_OPCO','SSM_RUN_INTERVAL_NAME', 'SSM_LAST_RUN_AEP_USAGE_DT_TO_NAME', 'S3_NONVEE_CONSUME','S3_CTIES_CONSUME', 'VAR_SNS_TOPIC'])


# get sns_client
sns_client=boto3.client('sns')
ssm_client = boto3.client('ssm')
glue_client = boto3.client('glue')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

fnLog("INFO", f" args passed to job: {str(args)}")

spark_appname = args['JOB_NAME']

AWS_ENV = args['AWS_ENV']
aws_env=AWS_ENV.lower()
# aws_env="dev"

VAR_OPCO = args['VAR_OPCO'] 
var_opco=VAR_OPCO.lower()


S3_NONVEE_CONSUME = args['S3_NONVEE_CONSUME']
# s3_nonvee_consume = S3_NONVEE_CONSUME.lower() + "-" + aws_env
s3_nonvee_consume = "s3://aep-dl-consume-nonvee-prod" ## Hard Code for Now.
# print('DEBUG: s3_nonvee_consume',s3_nonvee_consume)
fnLog("INFO", f" s3_nonvee_consume: {s3_nonvee_consume}")

S3_CTIES_CONSUME = args['S3_CTIES_CONSUME']
# S3_CTIES_CONSUME = "s3://aep-datalake-consume"
s3_cties_consume = S3_CTIES_CONSUME.lower() + "-" + aws_env
# print('DEBUG: s3_cties_consume',s3_cties_consume)
fnLog("INFO", f"s3_cties_consume: {s3_cties_consume}")

VAR_SNS_TOPIC = args['VAR_SNS_TOPIC'] ## ARN of SNS Topic
VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]

##==============================================================================================##
## check RUN_TYPE value from ssm and define VAR_AEP_USAGE_DT_FROM/VAR_AEP_USAGE_DT_TO accordingly
## Get last_run_end_dt from ssm parameter which will be VAR_AEP_USAGE_DT_FROM for current run
VAR_AEP_USAGE_DT_FROM=''
VAR_AEP_USAGE_DT_TO=''

ssm_run_interval_name=args['SSM_RUN_INTERVAL_NAME']
ssm_run_interval_resp = ssm_client.get_parameter(Name=ssm_run_interval_name, WithDecryption=False)
VAR_RUN_INTERVAL_VAL = int(ssm_run_interval_resp['Parameter']['Value'])  ## default is 14

# ssm_run_type_name=args['SSM_RUN_TYPE_NAME'] ## should be 's' for scheduled
# ssm_run_type_resp = ssm_client.get_parameter(Name=ssm_run_type_name, WithDecryption=False)
# SSM_RUN_TYPE_VAL = ssm_run_type_resp['Parameter']['Value'] 

##### create ssm_last_run_aep_usage_dt_to_name(<OPCO>) if not exists
try:
    ssm_last_run_aep_usage_dt_to_name=str(args['SSM_LAST_RUN_AEP_USAGE_DT_TO_NAME']).lower() + '_' + var_opco
    ssm_client.get_parameter(Name=ssm_last_run_aep_usage_dt_to_name, WithDecryption=False)
    parameter_exists=True
except ssm_client.exceptions.ParameterNotFound:
    parameter_exists=False
if not parameter_exists:
    ssm_client.put_parameter(Name=ssm_last_run_aep_usage_dt_to_name, Value= ' ' , Type='String', Overwrite=True )
    print(f"successfully created ssm parameter: {ssm_last_run_aep_usage_dt_to_name}")
else:
    print(f"already exists ssm parameter: {ssm_last_run_aep_usage_dt_to_name}")


# ssm_last_run_aep_usage_dt_to_name=args['SSM_LAST_RUN_AEP_USAGE_DT_TO_NAME']
ssm_last_run_aep_usage_dt_to_resp = ssm_client.get_parameter(Name=ssm_last_run_aep_usage_dt_to_name, WithDecryption=False)
SSM_LAST_RUN_AEP_USAGE_DT_TO_VAL = ssm_last_run_aep_usage_dt_to_resp['Parameter']['Value']

var_today=date.today()
var_delta=timedelta(days=VAR_RUN_INTERVAL_VAL)

# if SSM_RUN_TYPE_VAL.lower() == 's' and SSM_LAST_RUN_AEP_USAGE_DT_TO_VAL.strip() == '':  
if SSM_LAST_RUN_AEP_USAGE_DT_TO_VAL.strip() == '':      

    var_end_date = var_today - var_delta
    VAR_AEP_USAGE_DT_TO = str(var_end_date)

    var_start_date = var_end_date - var_delta
    VAR_AEP_USAGE_DT_FROM = str(var_start_date)

else:

    VAR_AEP_USAGE_DT_FROM = SSM_LAST_RUN_AEP_USAGE_DT_TO_VAL
    date_obj=datetime.strptime(VAR_AEP_USAGE_DT_FROM, '%Y-%m-%d').date()            
    var_end_date = date_obj + var_delta
    VAR_AEP_USAGE_DT_TO = str(var_end_date)
    ## To check the schedule is running every alternate saturday
    # var_today = datetime.strptime('2023-03-28', '%Y-%m-%d').date() ## for checking
    ##############commented below for schedule
    print('var_end_date:',str(var_end_date))
    print('var_today:',str(var_today))
    if (var_end_date - var_today) < (var_delta):        
        print('Exiting the job process as it ran before scheduled time..')
        os._exit(0)

# VAR_AEP_USAGE_DT_FROM=args['VAR_AEP_USAGE_DT_FROM']
# VAR_AEP_USAGE_DT_TO=args['VAR_AEP_USAGE_DT_TO']
# print('DEBUG: VAR_AEP_USAGE_DT_FROM: ',VAR_AEP_USAGE_DT_FROM)
# print('DEBUG: VAR_AEP_USAGE_DT_TO: ',VAR_AEP_USAGE_DT_TO)

fnLog("INFO", f" VAR_AEP_USAGE_DT_FROM: {VAR_AEP_USAGE_DT_FROM}")
fnLog("INFO", f" VAR_AEP_USAGE_DT_TO: {VAR_AEP_USAGE_DT_TO}")


nonvee_consume_basePath=s3_nonvee_consume+"/util/intervals/reading_ivl_nonvee_"+var_opco

cties_voltage_summary_basePath=s3_cties_consume + "/cties/meter_xfmr_voltg_summ"  

cties_xfmr_meter_map_basePath=s3_cties_consume+"/cties/meter_xfmr_mapping/"
cties_xfmr_meter_map_path=cties_xfmr_meter_map_basePath+"aep_opco="+var_opco

##==============================================================================================##
## Setup Spark Config -- 

sc = SparkContext()
glueContext = GlueContext(sc.getOrCreate())
spark = glueContext.spark_session

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

## sc = spark.sparkContext
## glueContext = GlueContext(sc.getOrCreate())

sqlContext = SQLContext(sc, spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# job.commit()
conf=SparkConf()

current_user1 = sqlContext._sc.sparkUser()

##==============================================================================================##
## User defined functions for comparison calculations

norm_udf = F.udf(lambda x: DenseVector(x).norm(2).item(), FloatType())

def normalized_array(x, norm):
    return Vectors.dense(x) / norm

def euclidean_distance(x, y):
    return sqrt(Vectors.squared_distance(x, y)) 

def cosine_sim(x, y):
    xx = DenseVector(x)
    yy = DenseVector(y)
    return float(xx.dot(yy) / ( xx.norm(2) * yy.norm(2) ))

def shape_mismatch(x, y):
    x_last = 0
    y_last = 0
    c = 0
    for i in range(len(x)):
        if (x[i] >= x_last and y[i] >= y_last) or (x[i] < x_last and y[i] < y_last):
            x_last = x[i]
            y_last = y[i]
            continue
        else:
            c += 1
            x_last = x[i]
            y_last = y[i]
    return c

##==============================================================================================##
## Initialize the Run Parameters and Such


## Define the NONVEE Parquet Schema
# try:
nonvee_consume_schema=StructType([\
StructField("serialnumber", StringType(), True),\
StructField("source", StringType(), True),\
StructField("aep_devicecode", StringType(), True),\
StructField("isvirtual_meter", StringType(), True),\
StructField("timezoneoffset", StringType(), True),\
StructField("aep_premise_nb", StringType(), True),\
StructField("aep_service_point", StringType(), True),\
StructField("aep_mtr_install_ts", StringType(), True),\
StructField("aep_mtr_removal_ts", StringType(), True),\
StructField("aep_srvc_dlvry_id", StringType(), True),\
StructField("aep_comp_mtr_mltplr", DoubleType(), True),\
StructField("name_register", StringType(), True),\
StructField("isvirtual_register", StringType(), True),\
StructField("toutier", StringType(), True),\
StructField("toutiername", StringType(), True),\
StructField("aep_srvc_qlty_idntfr", StringType(), True),\
StructField("aep_channel_id", StringType(), True),\
StructField("aep_raw_uom", StringType(), True),\
StructField("aep_sec_per_intrvl", DoubleType(), True),\
StructField("aep_meter_alias", StringType(), True),\
StructField("aep_meter_program", StringType(), True),\
StructField("aep_billable_ind", StringType(), True),\
StructField("aep_usage_type", StringType(), True),\
StructField("aep_timezone_cd", StringType(), True),\
StructField("endtimeperiod", StringType(), True),\
StructField("starttimeperiod", StringType(), True),\
StructField("value", FloatType(), True),\
StructField("aep_raw_value", FloatType(), True),\
StructField("scalarfloat", FloatType(), True),\
StructField("aep_data_quality_cd", StringType(), True),\
StructField("aep_data_validation", StringType(), True),\
StructField("aep_acct_cls_cd", StringType(), True),\
StructField("aep_acct_type_cd", StringType(), True),\
StructField("aep_mtr_pnt_nb", StringType(), True),\
StructField("aep_tarf_pnt_nb", StringType(), True),\
StructField("aep_endtime_utc", StringType(), True),\
StructField("aep_city", StringType(), True),\
StructField("aep_zip", StringType(), True),\
StructField("aep_state", StringType(), True),\
StructField("hdp_update_user", StringType(), True),\
StructField("hdp_insert_dttm", TimestampType(), True),\
StructField("hdp_update_dttm", TimestampType(), True),\
StructField("authority", StringType(), True),\
StructField("aep_derived_uom", StringType(), True),\
StructField("aep_opco", StringType(), True),\
StructField("aep_usage_dt", StringType(), True),\
StructField("aep_meter_bucket", StringType(), True)\
])

meter_xfmr_mapping_schema=StructType([\
StructField("aep_premise_nb", StringType(), True),\
StructField("serialnumber", StringType(), True),\
StructField("aep_premise_loc_nb", StringType(), True),\
StructField("aep_premise_long", StringType(), True),\
StructField("aep_premise_lat", StringType(), True),\
StructField("trsf_pole_nb", StringType(), True),\
StructField("xfmr_long", StringType(), True),\
StructField("xfmr_lat", StringType(), True),\
StructField('trsf_mount_cd',StringType(),True),\
StructField("gis_circuit_nb", StringType(), True),\
StructField("gis_circuit_nm", StringType(), True),\
StructField("gis_station_nb", StringType(), True),\
StructField("gis_station_nm", StringType(), True),\
StructField("company_cd", StringType(), True),\
StructField("srvc_entn_cd", StringType(), True),\
StructField("distance_feets", DoubleType(), True),\
StructField("county_cd", StringType(), True),\
StructField("county_nm", StringType(), True),\
StructField("district_nb", StringType(), True),\
StructField("district_nm", StringType(), True),\
StructField("type_srvc_cd", StringType(), True),\
StructField("type_srvc_cd_desc", StringType(), True),\
StructField("srvc_addr_1_nm", StringType(), True),\
StructField("srvc_addr_2_nm", StringType(), True),\
StructField("srvc_addr_3_nm", StringType(), True),\
StructField("srvc_addr_4_nm", StringType(), True),\
StructField("serv_city_ad", StringType(), True),\
StructField("state_cd", StringType(), True),\
StructField("serv_zip_ad", StringType(), True),\
StructField("aws_update_dttm", StringType(), True),\
StructField("run_control_id", LongType(), True),\
StructField("aep_opco", StringType(), True),\
StructField("asof_dt", StringType(), True)\
])

##==============================================================================================##
## Loop through the dates and prepare the Partition List

VAR_AEP_USAGE_DATES = pd.date_range(start=VAR_AEP_USAGE_DT_FROM, end=VAR_AEP_USAGE_DT_TO, freq='D').strftime("%Y-%m-%d")
nonvee_consume_paths=[]
for usage_dt in VAR_AEP_USAGE_DATES:
    nonvee_consume_paths = nonvee_consume_paths + [ os.path.join(nonvee_consume_basePath, f"aep_opco={VAR_OPCO}", f"aep_usage_dt={usage_dt}") ]


fnLog("INFO", "\n\n")
fnLog("INFO", f" VAR_AEP_USAGE_DATES: <{str(VAR_AEP_USAGE_DATES)}>")
fnLog("INFO", f" nonvee_consume_basePath: <{str(nonvee_consume_basePath)}>")
fnLog("INFO", f" nonvee_consume_paths: <{str(nonvee_consume_paths)}>")
fnLog("INFO", f"\n\n")



##==============================================================================================##
## Initialize the Run Parameters and Such

nonvee_consume=spark.read.schema(nonvee_consume_schema) \
.option("basePath", nonvee_consume_basePath) \
.option("inferSchema", "false") \
.option("compression", "snappy") \
.option("mergeSchema", "false") \
.format("parquet") \
.load(path=nonvee_consume_paths)

if VAR_OPCO =='tx' :
    nonvee_consume=nonvee_consume \
    .filter( (nonvee_consume.aep_derived_uom == 'VOLT') & ( nonvee_consume.aep_raw_uom.isin('instva1neutral') ) )

else:
    nonvee_consume=nonvee_consume \
    .filter( (nonvee_consume.aep_derived_uom == 'VOLT') & ( nonvee_consume.aep_raw_uom.isin('vrmsa-n', 'vavga-n') ) )

## .filter( (nonvee_consume.aep_derived_uom == 'VOLT') & (nonvee_consume.aep_srvc_qlty_idntfr == 'AVG') & (nonvee_consume.aep_raw_uom == 'vavga-n') & (nonvee_consume.name_register == 'E-VOLTAGE-15-AVG-A') )

nonvee_consume=nonvee_consume \
.select("aep_opco", "aep_derived_uom", "serialnumber", "starttimeperiod", "aep_channel_id", "aep_raw_uom", "aep_raw_value")

##==============================================================================================##
## We have Bad Data -- Off 15 min Reads on following SerialNumbers - Skip them for now. // CHECK

nonvee_consume=nonvee_consume.withColumn("round15min_check", unix_timestamp(concat(substring('starttimeperiod',1,10), substring('starttimeperiod',12,8)), 'yyyy-MM-ddHH:mm:ss') % (15 * 60) )
nonvee_consume=nonvee_consume.filter(nonvee_consume.round15min_check == 0)
nonvee_consume=nonvee_consume.drop('round15min_check')

##==============================================================================================##

nonvee_consume=nonvee_consume.withColumn("hhmm", concat(lit("start_"), substring("starttimeperiod", 12,2), substring("starttimeperiod", 15,2)))
nonvee_consume=nonvee_consume.withColumn("aep_usage_dt", substring("starttimeperiod",1,10))
nonvee_consume=nonvee_consume.withColumn("has_nulls", when(isnull("aep_raw_value"),lit(1)).otherwise(lit(0)))
nonvee_consume=nonvee_consume.drop('starttimeperiod')

print("DEBUG: NonVEE Consume Data - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
## print("DEBUG: NonVEE Consume - Partitions = "  + str(nonvee_consume.rdd.getNumPartitions()))
## nonvee_consume.printSchema()


##==============================================================================================##
## We need ALL 96 Row / day
## We Need Voltages to be filled on all 96 rows i.e. > 1.0
## We need NO Nulls on any of the 96 Reads

# group_df=nonvee_consume.groupBy(['serialnumber', 'aep_usage_dt'])
group_df=nonvee_consume.groupBy(['serialnumber', 'aep_usage_dt', 'aep_channel_id', 'aep_raw_uom'])
daily96 = group_df.agg({"*": "count", "has_nulls": "sum", "aep_raw_value": "min"})
daily96 = daily96.withColumnRenamed('count(1)', 'rowcnt').withColumnRenamed('sum(has_nulls)', 'has_nulls').withColumnRenamed('min(aep_raw_value)', 'low_read')
daily96 = daily96.filter( (daily96.rowcnt == 96) & (daily96.has_nulls == 0) & (daily96.low_read > 0) )
daily96 = daily96.drop('rowcnt', 'has_nulls', 'low_read')
daily96 = daily96.withColumnRenamed('serialnumber', 'good_serialnumber')\
    .withColumnRenamed('aep_usage_dt', 'good_aep_usage_dt')\
    .withColumnRenamed('aep_channel_id', 'good_aep_channel_id')\
    .withColumnRenamed('aep_raw_uom', 'good_aep_raw_uom')
## daily96.persist()
daily96.printSchema()


##==============================================================================================##
## Connect back with Base NonVee Consume Frame to bypass the "bad" reads

join_cond=[nonvee_consume.serialnumber == daily96.good_serialnumber, nonvee_consume.aep_usage_dt == daily96.good_aep_usage_dt, nonvee_consume.aep_channel_id == daily96.good_aep_channel_id, nonvee_consume.aep_raw_uom == daily96.good_aep_raw_uom]

nonvee_consume = nonvee_consume.join(daily96, on=join_cond, how="left_outer")
nonvee_consume = nonvee_consume.filter(nonvee_consume.good_serialnumber.isNotNull() | nonvee_consume.good_aep_usage_dt.isNotNull())
nonvee_consume = nonvee_consume.drop('has_nulls', 'good_serialnumber', 'good_aep_usage_dt', 'good_aep_channel_id', 'good_aep_raw_uom')

##==============================================================================================##
## Load the Transformer - Meter Premise Mapping

## If we know the LAST RUN DATE by OPCo - We can then add asof_dt>=Last Run Date as a criteria for partition Pruning

mp2=spark.read.schema(meter_xfmr_mapping_schema) \
.option("basePath", cties_xfmr_meter_map_basePath) \
.option("inferSchema", "false") \
.option("compression", "snappy") \
.option("mergeSchema", "false") \
.format("parquet") \
.load(path=cties_xfmr_meter_map_path)

mp2.createOrReplaceTempView("meter_xfmr_mapping")

mp_sql = "select DISTINCT aep_premise_nb, serialnumber, trsf_pole_nb,trsf_mount_cd, gis_circuit_nb, gis_circuit_nm, gis_station_nb, gis_station_nm, company_cd, srvc_entn_cd, distance_feets, asof_dt from meter_xfmr_mapping where aep_opco=" + f"'{VAR_OPCO}'" 
mp_sql = mp_sql + " and asof_dt = (select max(asof_dt) from meter_xfmr_mapping where aep_opco=" + f"'{VAR_OPCO}')"

mp=spark.sql(mp_sql)

print("DEBUG: Meter Transformer mp - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
mp.printSchema()
# print("DEBUG: mp - count = "  + str(mp.count()))

mp_xfmr_mtr_cnt=mp.groupBy('trsf_pole_nb').agg(count(mp.serialnumber).alias('xf_meter_cnt'))
mp_xfmr_mtr_cnt=mp_xfmr_mtr_cnt.withColumnRenamed('trsf_pole_nb', 'xtrsf_pole_nb')
mp_final=mp.join(mp_xfmr_mtr_cnt, (mp.trsf_pole_nb == mp_xfmr_mtr_cnt.xtrsf_pole_nb), how="inner")
mp_final=mp_final.drop('xtrsf_pole_nb')

##==============================================================================================##
## Join the mp_final Frame back to our Base Consume NonVEE Frame

mp_nonvee_consume = nonvee_consume.join(mp_final, "serialnumber", how="inner")
mp_nonvee_consume.printSchema()  


##==============================================================================================##
## Pivot the Base NonVee Consume Frame

pivot_mp_nonvee_df = mp_nonvee_consume.groupBy(['aep_opco', 'aep_derived_uom', 'serialnumber', 'aep_premise_nb', 'aep_usage_dt', 'trsf_pole_nb','trsf_mount_cd',  'gis_circuit_nb', 'gis_circuit_nm', 'gis_station_nb', 'gis_station_nm', 'company_cd', 'srvc_entn_cd','distance_feets','xf_meter_cnt', 'aep_raw_uom', 'aep_channel_id', 'asof_dt']) \
.pivot('hhmm') \
.agg(first('aep_raw_value'))

##==============================================================================================##
## Pivot intervals and throw them into an array. 

## Select the Columns for Array Storage
array_cols = [c for c in pivot_mp_nonvee_df.columns if c.startswith("start_")]
pivot_mp_nonvee_df = pivot_mp_nonvee_df.withColumn('read_array', F.array(array_cols))
base_voltage_df = pivot_mp_nonvee_df.select('aep_premise_nb', 'serialnumber', 'srvc_entn_cd', 'trsf_pole_nb', 'trsf_mount_cd','gis_circuit_nb', 'gis_circuit_nm', 'gis_station_nb', 'gis_station_nm', 'company_cd', 'srvc_entn_cd','distance_feets', 'xf_meter_cnt', 'aep_derived_uom',  'aep_raw_uom', 'aep_channel_id', 'asof_dt', 'read_array', 'aep_opco', 'aep_usage_dt')

base_voltage_df.persist()
print("DEBUG: Voltage Array Data Frame - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
base_voltage_df.printSchema()


##==============================================================================================##
## Collect Transformer Level Averages - ALL Meters on that Transformer

transformer_avgs_df = base_voltage_df \
.groupby("trsf_pole_nb", "aep_usage_dt") \
.agg(F.array(*[F.avg(F.col("read_array")[i]).astype(FloatType()) for i in range(96)]) \
.alias("xf_read_array_whole"))

transformer_avgs_df.persist()
print("DEBUG: Transformer Averages Data Frame - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
# transformer_avgs_df.printSchema()


##==============================================================================================##
## Each Meter Would need to have its own Voltage Array 
## + It should also have AVG of All the Other Meters on that same transfomer
## Self join to get all other meter arrays on the transformer then average them at each interval
## Almost a Cartesian Product

meter_df1 = base_voltage_df.alias("meter_df1")
meter_df2 = base_voltage_df.alias("meter_df2")

cartesian_meter_df = meter_df1.join(meter_df2, ['trsf_pole_nb', 'aep_usage_dt']) ## Join to itself
cartesian_meter_df = cartesian_meter_df.filter( ~ (col('meter_df1.serialnumber') == col('meter_df2.serialnumber') ) ) ## Skip the Rows with Same Serial Numbers

cartesian_meter_df = cartesian_meter_df \
.select( \
meter_df1.aep_premise_nb, \
meter_df1.serialnumber, \
meter_df1.trsf_pole_nb, \
meter_df1.trsf_mount_cd, \
meter_df1.gis_circuit_nb, \
meter_df1.gis_circuit_nm, \
meter_df1.gis_station_nb, \
meter_df1.gis_station_nm, \
meter_df1.company_cd, \
meter_df1.srvc_entn_cd, \
meter_df1.distance_feets, \
meter_df1.xf_meter_cnt, \
meter_df1.aep_derived_uom, \
meter_df1.aep_raw_uom, \
meter_df1.aep_channel_id, \
meter_df1.asof_dt, \
meter_df1.read_array, \
col('meter_df2.read_array').alias('xf_read_array'), \
meter_df1.aep_opco, \
meter_df1.aep_usage_dt )



print("DEBUG: cartesian_meter_df - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
## print("DEBUG: cartesian_meter_df - Partitions = "  + str(cartesian_meter_df.rdd.getNumPartitions()))
# cartesian_meter_df.printSchema()


##==============================================================================================##
## Next Calculate AVG for Rest of the Meters on that XFMR 

xfmr_df = cartesian_meter_df \
.groupby('aep_premise_nb', 'serialnumber', 'trsf_pole_nb', 'trsf_mount_cd','gis_circuit_nb', 'gis_circuit_nm', 'gis_station_nb', 'gis_station_nm','company_cd','srvc_entn_cd', 'distance_feets', 'xf_meter_cnt', 'aep_derived_uom', 'aep_raw_uom', 'aep_channel_id', 'asof_dt', 'read_array', 'aep_opco', 'aep_usage_dt') \
.agg(F.array(*[F.avg(F.col("xf_read_array")[i]).astype(FloatType()) for i in range(96)]) \
.alias("xf_read_array"))

print("DEBUG: xfmr_df (Candidate Meter + AVG for Rest of the Meters) - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
## print("DEBUG: xfmr_df (Candidate Meter + AVG for Rest of the Meters) - Partitions = "  + str(xfmr_df.rdd.getNumPartitions()))
xfmr_df.printSchema()


##==============================================================================================##
## Calculate transformer average without removing any serial numbers
## Note: We already have Transformer Level Averages [without removing any meters] On -- transformer_avgs_df

## Now join the read array whole data frame from above back to the main dataset


xfmr_df = xfmr_df.alias("x")\
.join(transformer_avgs_df.alias("t"), ['trsf_pole_nb', 'aep_usage_dt']) \
.select("x.*", "t.xf_read_array_whole")

print("DEBUG: xfmr_df (Candidate Meter + AVG for Rest of the Meters + XFMR AVG ) - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
xfmr_df.printSchema()


xfmr_df = xfmr_df \
.select('aep_premise_nb', 'serialnumber', 'trsf_pole_nb', 'trsf_mount_cd', 'gis_circuit_nb', 'gis_circuit_nm', 'gis_station_nb', 'gis_station_nm', 'company_cd', 'srvc_entn_cd','distance_feets', 'xf_meter_cnt', 'aep_derived_uom', 'aep_raw_uom', 'aep_channel_id', 'asof_dt', 'read_array', 'xf_read_array', 'xf_read_array_whole', 'aep_opco', 'aep_usage_dt')

xfmr_df = xfmr_df.repartition('aep_opco', 'aep_usage_dt').sortWithinPartitions( ['trsf_pole_nb', 'serialnumber'] )
xfmr_df.persist()

print("DEBUG: xfmr_df (Candidate Meter + AVG for Rest of the Meters + XFMR AVG ) - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
## print("DEBUG: xfmr_df (Candidate Meter + AVG for Rest of the Meters + XFMR AVG ) - Partitions = "  + str(xfmr_df.rdd.getNumPartitions()))
# xfmr_df.printSchema()


##==============================================================================================##
# Apply UDFs to each array pairing  
## Normalize the Arrays before Calculation of Eucleadean Distance, Cosine Similarity, Shape mismatch Counts  

semifinal_df = xfmr_df \
.withColumn("array_norm", norm_udf("read_array")) \
.withColumn("xf_array_norm", norm_udf("xf_read_array")) \
.withColumn("array_normalized", F.udf(normalized_array, VectorUDT())("read_array", "array_norm")) \
.withColumn("xf_normalized", F.udf(normalized_array, VectorUDT())("xf_read_array", "xf_array_norm")) \
.withColumn("eucl_dist", F.udf(euclidean_distance, FloatType())("array_normalized", "xf_normalized")) \
.withColumn("cos_sim", F.udf(cosine_sim, FloatType())("array_normalized", "xf_normalized")) \
.withColumn("shape_mismatch_cnt", F.udf(shape_mismatch, IntegerType())("read_array", "xf_read_array"))


##==============================================================================================##
## Final Table
## Note that we are not saving the Normalized Arrays - 

semifinal_df = semifinal_df \
.select('aep_premise_nb', 'serialnumber', 'trsf_pole_nb', 'trsf_mount_cd', 'gis_circuit_nb', 'gis_circuit_nm', 'gis_station_nb', 'gis_station_nm', 'company_cd', 'srvc_entn_cd','distance_feets' ,'xf_meter_cnt', 'aep_derived_uom', 'aep_raw_uom', 'aep_channel_id', 'asof_dt', 'read_array', 'xf_read_array', 'xf_read_array_whole', 'eucl_dist', 'cos_sim', 'shape_mismatch_cnt', 'aep_opco', 'aep_usage_dt')


print("DEBUG: Done with UDF Semi-Final DF Ready - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
semifinal_df.printSchema()

##==============================================================================================##
## Z Score Calculations
## Collect the Mean & Standard Deviation of Eucleadean Distance and Shape Mismatch Counts for each Transformer

z_grouping_df = semifinal_df \
.filter(semifinal_df.xf_meter_cnt > 2) \
.groupBy( ['trsf_pole_nb', 'aep_usage_dt'] )

xmfr_df_mean = z_grouping_df \
.agg({"eucl_dist": "avg", "shape_mismatch_cnt": "avg"}) \
.withColumnRenamed('avg(eucl_dist)', 'mean_eucl_dist') \
.withColumnRenamed('avg(shape_mismatch_cnt)', 'mean_shape_mismatch_cnt')

xmfr_df_stddev = z_grouping_df \
.agg({"eucl_dist": "stddev", "shape_mismatch_cnt": "stddev"}) \
.withColumnRenamed('stddev(eucl_dist)', 'stddev_eucl_dist') \
.withColumnRenamed('stddev(shape_mismatch_cnt)', 'stddev_shape_mismatch_cnt')

z_score_base_df = xmfr_df_mean.join(xmfr_df_stddev, ['trsf_pole_nb', 'aep_usage_dt'], how="inner")


##==============================================================================================##
## connect it back to our Finalized DF

z_final_df = semifinal_df.alias("sf") \
.join(z_score_base_df.alias("z") , ['trsf_pole_nb', 'aep_usage_dt'], how="left_outer")


z_final_df = z_final_df \
.withColumn('z_score_eucl_dist', ( z_final_df.eucl_dist - z_final_df.mean_eucl_dist ) / z_final_df.stddev_eucl_dist ) \
.withColumn('z_score_shape_mismatch_cnt', ( z_final_df.shape_mismatch_cnt - z_final_df.mean_shape_mismatch_cnt ) / z_final_df.stddev_shape_mismatch_cnt )

print("DEBUG: Done with Z Score Calculations DF Ready - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
z_final_df.printSchema()


##==============================================================================================##
## Now Calculate the Median Values 


df_median_calc_groups = z_final_df \
.groupBy( ['aep_premise_nb', 'serialnumber', 'trsf_pole_nb', 'xf_meter_cnt'] )

median_calc_df = df_median_calc_groups.agg( \
percentile_approx('eucl_dist', 0.5).alias('median_eucl_dist'), \
percentile_approx('shape_mismatch_cnt', 0.5).alias('median_shape_mismatch_cnt'), \
percentile_approx('z_score_eucl_dist', 0.5).alias('median_z_score_eucl_dist') \
)

## connect it back to our Finalized DF With Z Scores + the Median Values

final_df = z_final_df.alias("zf") \
.join(median_calc_df.alias("m") , ['aep_premise_nb', 'serialnumber', 'trsf_pole_nb', 'xf_meter_cnt'], how="left_outer")


print("DEBUG: Done with Median Calculations DF Ready - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
final_df.printSchema()



##==============================================================================================##
## cties_voltage_summary_basePath
# Now write the final results. Partitioned by opco, date, uom

print("DEBUG: Ready to Write out the DF  - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

final_df = final_df \
.withColumn('aws_update_dttm', lit(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))) 

final_df = final_df \
.withColumn('run_control_id', unix_timestamp(final_df.aws_update_dttm , 'yyyy-MM-dd HH:mm:ss')) \
.withColumnRenamed('asof_dt', 'gis_mapping_asof_dt') \
.select( \
'aep_premise_nb', \
'serialnumber', \
'trsf_pole_nb', \
'trsf_mount_cd', \
'gis_circuit_nb', \
'gis_circuit_nm', \
'gis_station_nb', \
'gis_station_nm', \
'company_cd', \
'srvc_entn_cd', \
'distance_feets',\
'xf_meter_cnt', \
'aep_derived_uom',  \
'aep_raw_uom', \
'aep_channel_id', \
'gis_mapping_asof_dt', \
'read_array', \
'xf_read_array', \
'xf_read_array_whole', \
'eucl_dist', \
'cos_sim', \
'shape_mismatch_cnt', \
'mean_eucl_dist', \
'mean_shape_mismatch_cnt', \
'stddev_eucl_dist', \
'stddev_shape_mismatch_cnt', \
'z_score_eucl_dist', \
'z_score_shape_mismatch_cnt', \
'median_eucl_dist', \
'median_shape_mismatch_cnt', \
'median_z_score_eucl_dist', \
'aws_update_dttm', \
'run_control_id', \
'aep_opco', \
'aep_usage_dt' )

print("DEBUG: Final DF Ready - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
# final_df.printSchema()

##==============================================================================================##
## cties_voltage_summary_basePath
# Now write the final results. Partitioned by opco, date, uom

final_df = final_df.repartition('aep_opco', 'aep_usage_dt').sortWithinPartitions( ['trsf_pole_nb', 'serialnumber'] )
print("DEBUG: Done with Repartition the DF  - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

    
final_df.write.mode("overwrite") \
.partitionBy("aep_opco", "aep_usage_dt") \
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
.save(cties_voltage_summary_basePath)

fnLog("INFO", " DEBUG: Done with Writing out the DF  - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
fnLog("INFO", " Partition " + cties_voltage_summary_basePath + " successfully loaded.")
fnLog("INFO", " Load succeeded in " + str(time() - t) + " seconds.")

##==============================================================================================##
## MSCK
print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table cties.meter_xfmr_voltg_summ") ##to be modified

fnLog("INFO", " cties.meter_xfmr_voltg_summ successfully loaded.")

##==============================================================================================##
## if everything is ok, then set ssm_last_run_aep_usage_dt_to_name and ssm_run_type_name value for next run

ssm_last_run_aep_usage_dt_to_resp = ssm_client.put_parameter(Name=ssm_last_run_aep_usage_dt_to_name, Value= VAR_AEP_USAGE_DT_TO , Type='String', Overwrite=True )
# ssm_run_type_resp = ssm_client.put_parameter(Name=ssm_run_type_name, Value='s' , Type='String', Overwrite=True )

## Stop the Context

job.commit()
sc.stop()
# except Exception as e:
#     print(e)
#     msg_title=f"GlueJob: cties-meter-xfmr-voltg-summ failed"
#     print(msg_title)
#     msg_body=f"## GlueJob: cties-meter-xfmr-voltg-summ.py failed while processing for OPCO: <{VAR_OPCO}> for date range {VAR_AEP_USAGE_DT_FROM} and {VAR_AEP_USAGE_DT_TO}"
#     print(msg_body)
#     snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
#     raise e
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
##/**   V0.1      Diganta        11/21/2022  script for cties(stg to consume) glue-etl job     **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/
## s3://aep-datalake-apps-dev/hdpapp/cties/glueetl/cties-meter-xfmr-map-stg-consume.py
##==============================================================================================##
## Import Libraries 

from pyspark.context import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType, DateType, TimestampType, DecimalType
from pyspark.sql.functions import isnull, col, lit, when, to_date,unix_timestamp

import os
import sys
from datetime import datetime, timedelta
from pytz import timezone
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


##==============================================================================================##
## Collect Run Parameters 

## get sns client
sns_client=boto3.client('sns')
s3_resource = boto3.resource('s3') 
s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')
glue_client = boto3.client('glue')

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'AWS_ENV', 'VAR_OPCO', 'S3_DATA_WORK', 'S3_DATA_CONSUME','S3_DATA_TRANSFORM', 'VAR_SNS_TOPIC'])


AWS_ENV = args['AWS_ENV']
# AWS_ENV = "dev"
AWS_ENV = AWS_ENV.lower()

VAR_OPCO = args['VAR_OPCO'] ## "pso" 
## VAR_OPCO = "oh"
VAR_OPCO = VAR_OPCO.lower()

S3_DATA_WORK = args['S3_DATA_WORK']
s3_data_work = S3_DATA_WORK.lower() + "-" + AWS_ENV

S3_DATA_TRANSFORM = args["S3_DATA_TRANSFORM"] 
# s3_data_transform = S3_DATA_TRANSFORM.lower() + "-" + AWS_ENV
s3_data_transform = S3_DATA_TRANSFORM.lower() + "-" + "prod" ## hardcoded for prod

S3_DATA_CONSUME = args['S3_DATA_CONSUME']
s3_data_consume = S3_DATA_CONSUME.lower() + "-" + AWS_ENV

VAR_SNS_TOPIC = args['VAR_SNS_TOPIC'] ## ARN of SNS Topic
VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]
# print('DEBUG: VAR_SNS_TOPIC_SUCCESS:',VAR_SNS_TOPIC_SUCCESS)
# print('DEBUG: VAR_SNS_TOPIC_FAILURE',VAR_SNS_TOPIC_FAILURE)

####extract bucket_name from s3_data_consume and create basePath if already not exists
bucket_name = s3_data_consume[5:]

basePath_Prefix="cties/meter_xfmr_mapping/" 
try:
    s3_client.head_object(Bucket=bucket_name, Key=basePath_Prefix )
    print(f"{basePath_Prefix} already exists")
except:
    s3_client.put_object(Bucket=bucket_name, Key=basePath_Prefix )
    print(f"{basePath_Prefix} does not exist. Creating now..")
##==============================================================================================##
## Define basePaths


# xfmr_meter_map_consume_basePath = s3_data_consume + "/cties/meter_xfmr_mapping/" 
xfmr_meter_map_consume_basePath = s3_data_consume +"/" + basePath_Prefix 
print("DEBUG: xfmr_meter_map_consume_basePath - "  + xfmr_meter_map_consume_basePath) 

xfmr_meter_map_stg_basePath = s3_data_work + f"/util/cties/meter_xfmr_mapping_stg/"
xfmr_meter_map_stg_path = xfmr_meter_map_stg_basePath + f"aep_opco={VAR_OPCO}"

##meter_premise_vw
meter_premise_basePath=s3_data_work+f"/util/cties/meter_premise_stg/aep_opco={VAR_OPCO}"

##default_meter_premise
default_meter_premise_basePath= s3_data_transform + "/util/ods/meter_premise/hive_table" 

##==============================================================================================##
## Setup Spark Config 

spark_appname = args['JOB_NAME']
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

##==============================================================================================##
## Helper Functions

def getCompanyCode(aep_opco):
    co_cd_ownr = {
        "ap": ['01', '02', '06'],
        "im": ['04'],
        "kpc": ['03'],
        "oh": ['07', '10'],
        "pso": ['95'],
        "swp": ['96'],
        "tx": ['94', '97']
     }
    co_cd_ownr_filter = co_cd_ownr.get(aep_opco, "['XX']")  
    return co_cd_ownr_filter

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

####Get default_meter_premise data path with OPCO
co_cd_ownrs=getCompanyCode(VAR_OPCO)
default_meter_premise_paths = [] 
for co_cd_ownr in co_cd_ownrs:
    default_meter_premise_paths = default_meter_premise_paths + [ os.path.join( default_meter_premise_basePath,f"co_cd_ownr={co_cd_ownr}") ]
    
print('default_meter_premise_paths:',str(default_meter_premise_paths) )

# try:
##==============================================================================================##
## Define File Structure -- Stage Data
## Revised the # of Fields down to following fields.

stg_xfmr_mp_schema=StructType([
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
StructField('company_cd',StringType(),True),
StructField('srvc_entn_cd',StringType(),True), 
StructField('distance_feets',DoubleType(),True),
StructField('aws_update_dttm',StringType(),True),
StructField('run_control_id',LongType(),True),
StructField('aep_opco',StringType(),True),
StructField('asof_dt',StringType(),True)
])

stg_xfmr_mp=spark.read.schema(stg_xfmr_mp_schema) \
.option("basePath", xfmr_meter_map_stg_basePath) \
.option("inferSchema", "false") \
.option("compression", "snappy") \
.option("mergeSchema", "false") \
.format("parquet") \
.load(path=xfmr_meter_map_stg_path)

print("DEBUG: Meter Transformer mp - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
stg_xfmr_mp.printSchema()
print("DEBUG: _stg table record count: "  + str(stg_xfmr_mp.count()))

## meter_premise_schema
meter_premise_schema=StructType([\
    StructField("prem_nb", StringType(), True),\
    StructField("co_cd_ownr", StringType(), True),\
    StructField("state_cd", StringType(), True),\
    StructField("srvc_pnt_nm", StringType(), True),\
    StructField("mtr_pnt_nb", StringType(), True),\
    StructField("tarf_pnt_nb", StringType(), True),\
    StructField("premise_id", StringType(), True),\
    StructField("curr_bill_acct_nb", StringType(), True),\
    StructField("curr_bill_acct_id", StringType(), True),\
    StructField("curr_rvn_cls_cd", DoubleType(), True),\
    StructField("rvn_cls_cd_desc", StringType(), True),\
    StructField("mfr_devc_ser_nbr", StringType(), True),\
    StructField("mtr_stat_cd", StringType(), True),\
    StructField("mtr_stat_cd_desc", StringType(), True),\
    StructField("mtr_point_location", StringType(), True),\
    StructField("devc_stat_cd", StringType(), True),\
    StructField("devc_stat_cd_desc", StringType(), True),\
    StructField("devc_cd", StringType(), True),\
    StructField("devc_cd_desc", StringType(), True),\
    StructField("vintage_year", StringType(), True),\
    StructField("first_in_srvc_dt", DateType(), True),\
    StructField("phys_inst_dt", DateType(), True),\
    StructField("rmvl_ts", DateType(), True),\
    StructField("dial_cnst", DoubleType(), True),\
    StructField("bill_cnst", DoubleType(), True),\
    StructField("technology_tx", StringType(), True),\
    StructField("technology_desc", StringType(), True),\
    StructField("comm_cd", StringType(), True),\
    StructField("comm_desc", StringType(), True),\
    StructField("type_of_srvc_cd", StringType(), True),\
    StructField("type_of_srvc_cd_desc", StringType(), True),\
    StructField("type_srvc_cd", StringType(), True),\
    StructField("type_srvc_cd_desc", StringType(), True),\
    StructField("distributed_generation_flag", StringType(), True),\
    StructField("interval_data", StringType(), True),\
    StructField("intrvl_data_use_cd", StringType(), True),\
    StructField("intrvl_data_use_cd_desc", StringType(), True),\
    StructField("mtr_kind_cds", StringType(), True),\
    StructField("inst_ts timestamp", TimestampType(), True),\
    StructField("bill_fl", StringType(), True),\
    StructField("mfr_cd", StringType(), True),\
    StructField("mfr_cd_desc", StringType(), True),\
    StructField("last_fld_test_date", DecimalType(), True),\
    StructField("pgm_id_nm", StringType(), True),\
    StructField("aws_update_dttm", StringType(), True),\
    StructField("aep_opco", StringType(), True)\
])

##==============================================================================================##
## Load the Meter_Premise_VW data

meter_premise=spark.read.schema(meter_premise_schema) \
.option("inferSchema", "false") \
.option("compression", "snappy") \
.option("mergeSchema", "false") \
.format("parquet") \
.load(path=meter_premise_basePath) #mfr_devc_ser_nbr

meter_premise=meter_premise.select("mfr_devc_ser_nbr","prem_nb","aep_opco")
meter_premise=meter_premise.withColumnRenamed('aep_opco', 'mp_aep_opco')
    
meter_premise.printSchema()

#print("DEBUG: meter_premise record count: "  + str(meter_premise.count()))

##==============================================================================================##
## Load the Default_Meter_Premise data for county,district_cd and other srvc address details

##==============================================================================================##
## Load the Meter_Premise_VW data

default_meter_premise_schema=StructType([
StructField("premise_id",StringType(),True),
StructField("co_cd_ownr_desc",StringType(),True),
StructField("mfr_devc_ser_nbr",StringType(),True),
StructField("srvc_pnt_nm",StringType(),True),
StructField("devc_cd",StringType(),True),
StructField("mtr_stat_cd",StringType(),True),
StructField("mtr_stat_cd_desc",StringType(),True),
StructField("mtr_point_location",StringType(),True),
StructField("devc_stat_cd",StringType(),True),
StructField("devc_stat_cd_desc",StringType(),True),
StructField("devc_cd_desc",StringType(),True),
StructField("mtr_pnt_nb",StringType(),True),
StructField("vintage_year",StringType(),True),
StructField("first_in_srvc_dt",StringType(),True),
StructField("phys_inst_dt",StringType(),True),
StructField("rmvl_ts",StringType(),True),
StructField("dial_cnst",StringType(),True),
StructField("bill_cnst",StringType(),True),
StructField("inst_tod_cd",StringType(),True),
StructField("tod_mtr_fl",StringType(),True),
StructField("technology_tx",StringType(),True),
StructField("technology_desc",StringType(),True),
StructField("comm_cd",StringType(),True),
StructField("comm_desc",StringType(),True),
StructField("type_srvc_cd",StringType(),True),
StructField("type_srvc_cd_desc",StringType(),True),
StructField("interval_data",StringType(),True),
StructField("intrvl_data_use_cd",StringType(),True),
StructField("intrvl_data_use_cd_desc",StringType(),True),
StructField("mtr_kind_cds",StringType(),True),
StructField("inst_ts",StringType(),True),
StructField("bill_fl",StringType(),True),
StructField("mfr_cd",StringType(),True),
StructField("mfr_cd_desc",StringType(),True),
StructField("last_fld_test_date",StringType(),True),
StructField("pgm_id_nm",StringType(),True),
StructField("longitude",StringType(),True),
StructField("latitude",StringType(),True),
StructField("state_cd",StringType(),True),
StructField("state_cd_desc",StringType(),True),
StructField("jrsd_cd",StringType(),True),
StructField("jrsd_cd_descr",StringType(),True),
StructField("dvsn_cd",StringType(),True),
StructField("dvsn_cd_desc",StringType(),True),
StructField("area_cd",StringType(),True),
StructField("area_cd_desc",StringType(),True),
StructField("sub_area_cd",StringType(),True),
StructField("sub_area_cd_desc",StringType(),True),
StructField("prem_nb",StringType(),True),
StructField("esi_id",StringType(),True),
StructField("prem_stat_cd",StringType(),True),
StructField("prem_stat_cd_desc",StringType(),True),
StructField("frst_turn_on_dt",StringType(),True),
StructField("last_turn_off_dt",StringType(),True),
StructField("srvc_addr_1_nm",StringType(),True),
StructField("srvc_addr_2_nm",StringType(),True),
StructField("srvc_addr_3_nm",StringType(),True),
StructField("srvc_addr_4_nm",StringType(),True),
StructField("ser_half_ind_ad",StringType(),True),
StructField("serv_city_ad",StringType(),True),
StructField("serv_hous_nbr_ad",StringType(),True),
StructField("serv_ptdr_ad",StringType(),True),
StructField("serv_prdr_ad",StringType(),True),
StructField("addl_srv_data_ad",StringType(),True),
StructField("serv_st_name_ad",StringType(),True),
StructField("serv_st_dsgt_ad",StringType(),True),
StructField("serv_unit_dsgt_ad",StringType(),True),
StructField("serv_unit_nbr_ad",StringType(),True),
StructField("serv_zip_ad",StringType(),True),
StructField("st_cd_ad",StringType(),True),
StructField("route_nb_ad",StringType(),True),
StructField("rurl_rte_type_cd",StringType(),True),
StructField("rurl_rte_type_cd_desc",StringType(),True),
StructField("county_cd",StringType(),True),
StructField("county_nm",StringType(),True),
StructField("cumu_cd",StringType(),True),
StructField("cumu_cd_desc",StringType(),True),
StructField("building_type",StringType(),True),
StructField("building_type_desc",StringType(),True),
StructField("profile_id",StringType(),True),
StructField("hsng_ctgy_cd",StringType(),True),
StructField("hsng_ctgy_cd_desc",StringType(),True),
StructField("heat_typ_cd",StringType(),True),
StructField("heat_typ_cd_desc",StringType(),True),
StructField("heat_src_fuel_typ_cd",StringType(),True),
StructField("owns_home_cd",StringType(),True),
StructField("owns_home_cd_desc",StringType(),True),
StructField("squr_feet_mkt_qy",StringType(),True),
StructField("power_pool_cd",StringType(),True),
StructField("seasonal_fl",StringType(),True),
StructField("tax_dstc_cd",StringType(),True),
StructField("tax_dstc_cd_desc",StringType(),True),
StructField("tarf_pnt_nb",StringType(),True),
StructField("tarf_pt_stat_cd",StringType(),True),
StructField("tarf_pt_stat_cd_desc",StringType(),True),
StructField("type_of_srvc_cd",StringType(),True),
StructField("type_of_srvc_cd_desc",StringType(),True),
StructField("srvc_entn_cd",StringType(),True),
StructField("year_strc_cmpl_dt",StringType(),True),
StructField("srvc_pole_nb",StringType(),True),
StructField("trsf_pole_nb",StringType(),True),
StructField("delv_pt_cd",StringType(),True),
StructField("emrgncy_gen_fl",StringType(),True),
StructField("co_gen_fl",StringType(),True),
StructField("naics_cd",StringType(),True),
StructField("ami_ftprnt_cd",StringType(),True),
StructField("curr_bill_acct_id",StringType(),True),
StructField("curr_bill_acct_nb",StringType(),True),
StructField("curr_cust_nm",StringType(),True),
StructField("curr_acct_cls_cd",StringType(),True),
StructField("curr_tarf_cd",StringType(),True),
StructField("curr_tarf_cd_desc",StringType(),True),
StructField("curr_rvn_cls_cd",StringType(),True),
StructField("rvn_cls_cd_desc",StringType(),True),
StructField("cycl_nb",StringType(),True),
StructField("annual_kwh",StringType(),True),
StructField("annual_max_dmnd",StringType(),True),
StructField("wthr_stn_cd",StringType(),True),
StructField("dstrbd_gen_ind_cd",StringType(),True),
StructField("dstrbd_gen_typ_cd",StringType(),True),
StructField("dstrbd_gen_instl_dt",StringType(),True),
StructField("dstrbd_gen_capcty_nb",StringType(),True),
StructField("enrgy_dvrn_fl",StringType(),True),
StructField("enrgy_dvrn_cd",StringType(),True),
StructField("enrgy_dvrn_dt",StringType(),True),
StructField("curr_enrgy_efncy_prtcpnt_fl",StringType(),True),
StructField("curr_enrgy_efncy_pgm_dt",StringType(),True),
StructField("curr_enrgy_efncy_pgm_cd",StringType(),True),
StructField("directions",StringType(),True),
StructField("cmsg_mtr_mult_cd",StringType(),True),
StructField("oms_area",StringType(),True),
StructField("load_area_cd",StringType(),True),
StructField("latitude_nb",StringType(),True),
StructField("longitude_nb",StringType(),True),
StructField("circuit_nb",StringType(),True),
StructField("circuit_nm",StringType(),True),
StructField("station_nb",StringType(),True),
StructField("station_nm",StringType(),True),
StructField("xfmr_nb",StringType(),True),
StructField("xfmr_type",StringType(),True),
StructField("xfmr_name",StringType(),True),
StructField("district_nb",StringType(),True),
StructField("district_nm",StringType(),True),
StructField("co_cd_ownr",StringType(),True),
])

default_meter_premise_df =  spark.read.schema(default_meter_premise_schema) \
.option("basePath", default_meter_premise_basePath) \
.option("inferSchema", "false") \
.option("mergeSchema", "false") \
.option("delimiter", "\t") \
.format("csv") \
.load(path=default_meter_premise_paths)

##==============================================================================================##
## Select only required columns along with key column

default_meter_premise_df = default_meter_premise_df\
.select("mfr_devc_ser_nbr","county_cd","county_nm","district_nb","district_nm","type_srvc_cd","type_srvc_cd_desc","srvc_addr_1_nm","srvc_addr_2_nm","srvc_addr_3_nm","srvc_addr_4_nm","serv_city_ad","state_cd","serv_zip_ad").distinct()

##==============================================================================================##
## Set valid/Invalid Status

stg_xfmr_mp=stg_xfmr_mp.withColumn('is_invalid',\
when(col('aep_premise_nb').isNull(), lit(1))\
.when(col('trsf_pole_nb').isNull(), lit(1))\
.when(col('aep_premise_long').isNull(), lit(1))\
.when(col('aep_premise_lat').isNull(), lit(1))\
.otherwise(0))

##==============================================================================================##
## invalid records

stg_xfmr_mp_invalid=stg_xfmr_mp.filter(col("is_invalid") == 1)
print("DEBUG: invalid record count: "  + str(stg_xfmr_mp_invalid.count()))  

##==============================================================================================##
## valid records

stg_xfmr_mp=stg_xfmr_mp.filter(col("is_invalid") == 0)
stg_xfmr_mp_valid_count=stg_xfmr_mp.count()
print("DEBUG: valid record count: "  + str(stg_xfmr_mp_valid_count))   

stg_xfmr_mp=stg_xfmr_mp.drop('is_invalid')
##==============================================================================================##
## Drop 'aws_update_dttm', 'run_control_id' columns. De-Dup the stage data and again add back 'aws_update_dttm', 'run_control_id'

stg_xfmr_mp=stg_xfmr_mp.drop('aws_update_dttm', 'run_control_id')

stg_xfmr_mp = stg_xfmr_mp.distinct()

##==============================================================================================##
## Join meterp_remise_vw and meter_xfmr_mapping and get mfr_devc_ser_nbr as serialnumber

stg_xfmr_mp=stg_xfmr_mp.join(meter_premise, (stg_xfmr_mp.aep_premise_nb == meter_premise.prem_nb), how="inner")

stg_xfmr_mp = stg_xfmr_mp\
.select('aep_premise_nb',col("mfr_devc_ser_nbr").alias("serialnumber"),'aep_premise_loc_nb','aep_premise_long','aep_premise_lat','trsf_pole_nb','xfmr_long','xfmr_lat','trsf_mount_cd', 'gis_circuit_nb','gis_circuit_nm','gis_station_nb','gis_station_nm', 'company_cd','srvc_entn_cd','distance_feets','aep_opco','asof_dt')

##==============================================================================================##
## get distinct values

stg_xfmr_mp=stg_xfmr_mp.distinct()
print("DEBUG: after distinct valid record count: "  + str(stg_xfmr_mp.count()))
##==============================================================================================##
## Join default_meter_premise and meter_xfmr_mapping for county/district_cd and other srvc address details

# stg_xfmr_mp=stg_xfmr_mp.join(default_meter_premise_df, (stg_xfmr_mp.aep_premise_nb == default_meter_premise_df.prem_nb), how="inner")
stg_xfmr_mp=stg_xfmr_mp.join(default_meter_premise_df, (stg_xfmr_mp.serialnumber == default_meter_premise_df.mfr_devc_ser_nbr), how="left_outer")

stg_xfmr_mp = stg_xfmr_mp\
.select("aep_premise_nb","serialnumber","aep_premise_loc_nb","aep_premise_long","aep_premise_lat","trsf_pole_nb","xfmr_long","xfmr_lat","trsf_mount_cd", "gis_circuit_nb","gis_circuit_nm","gis_station_nb","gis_station_nm", "company_cd","srvc_entn_cd","distance_feets","county_cd","county_nm","district_nb","district_nm","type_srvc_cd","type_srvc_cd_desc","srvc_addr_1_nm","srvc_addr_2_nm","srvc_addr_3_nm","srvc_addr_4_nm","serv_city_ad","state_cd","serv_zip_ad","aep_opco","asof_dt")


stg_xfmr_mp = stg_xfmr_mp\
.withColumn('aws_update_dttm', lit(datetime.today().strftime("%Y-%m-%d %H:%M:%S")))

stg_xfmr_mp = stg_xfmr_mp\
.withColumn('run_control_id', unix_timestamp(stg_xfmr_mp.aws_update_dttm , 'yyyy-MM-dd HH:mm:ss')) 

stg_xfmr_mp = stg_xfmr_mp\
.select("aep_premise_nb","serialnumber","aep_premise_loc_nb","aep_premise_long","aep_premise_lat","trsf_pole_nb","xfmr_long","xfmr_lat","trsf_mount_cd", "gis_circuit_nb","gis_circuit_nm","gis_station_nb","gis_station_nm", "company_cd","srvc_entn_cd","distance_feets","county_cd","county_nm","district_nb","district_nm","type_srvc_cd","type_srvc_cd_desc","srvc_addr_1_nm","srvc_addr_2_nm","srvc_addr_3_nm","srvc_addr_4_nm","serv_city_ad","state_cd","serv_zip_ad","aws_update_dttm","run_control_id","aep_opco","asof_dt")

fnLog("INFO", " dispalying one record..")
fnLog("INFO", f"{str(stg_xfmr_mp.take(1))}")

##==============================================================================================##
## Repartition the Data

stg_xfmr_mp=stg_xfmr_mp.repartition("aep_opco", "asof_dt").sortWithinPartitions('trsf_pole_nb', 'serialnumber')
print("DEBUG: Done with Repartition the DF - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

##==============================================================================================##
## Write the Data Out to Target Folder

print("DEBUG: Starting To Write the Frame to XFMR Mapping Folder - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

stg_xfmr_mp.write.mode("overwrite") \
.partitionBy("aep_opco", "asof_dt") \
.option("parquet.bloom.filter.enabled#aep_premise_nb", "true")\
.option("parquet.bloom.filter.enabled#serialnumber", "true")\
.option("parquet.bloom.filter.enabled#trsf_pole_nb", "true")\
.option("parquet.bloom.filter.expected.ndv#aep_premise_nb", "5000000")\
.option("parquet.bloom.filter.expected.ndv#serialnumber", "5000000")\
.option("parquet.bloom.filter.expected.ndv#trsf_pole_nb", "2000000")\
.option("compression", "SNAPPY") \
.option("spark.sql.files.maxRecordsPerFile", 1000000) \
.format("parquet") \
.save(xfmr_meter_map_consume_basePath)

print("DEBUG: Done with Writing to XFMR Mapping Folder - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

##==============================================================================================##
## Will need MSCK after Spark is done.
print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table cties.meter_xfmr_mapping")

fnLog("INFO", " Partition " + xfmr_meter_map_consume_basePath + " successfully loaded.")
fnLog("INFO", " Load succeeded in " + str(round(time.time() - t,2)) + " seconds.")


## Stop the Context
job.commit()
sc.stop()
# except Exception as e:
#     print(e)
#     msg_title=f"GlueJob: cties-meter-xfmr-map-stg-consume failed"
#     print(msg_title)
#     msg_body=f"## GlueJob: cties-meter-xfmr-map-stg-consume.py failed while processing for OPCO: <{VAR_OPCO}>"
#     print(msg_body)
#     snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
#     raise e
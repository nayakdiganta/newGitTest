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
# /**   V0.1       Diganta                  05/14/2023      hist-load                          **/
# /**------------------------------------------------------------------------------------------**/

from pyspark.context import SparkContext, SparkConf

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)
from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list
import sys
import os

from datetime import datetime, timedelta , date # , timedelta
import json

import pandas as pd
import boto3
from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from functools import reduce

import logging

from time import time

t = time()

# get clients
sns_client = boto3.client('sns')
ssm_client = boto3.client('ssm')
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

def unionAll(*dfs):
    return reduce(DataFrame.unionAll,dfs)

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

args = getResolvedOptions( sys.argv, ["JOB_NAME", "AWS_ENV", "VAR_OPCO", "SSM_LAST_RUN_DT_NAME", "S3_DATA_TRANSFORM", "S3_DATA_WORK", "S3_DATA_CONSUME", "VAR_SNS_TOPIC"])

SPARK_APPNAME = args["JOB_NAME"]
AWS_ENV = args["AWS_ENV"]
AWS_ENV = AWS_ENV.lower()

VAR_OPCO = args["VAR_OPCO"]  
VAR_OPCO = VAR_OPCO.lower()

S3_DATA_TRANSFORM = args["S3_DATA_TRANSFORM"] 
# S3_DATA_TRANSFORM = S3_DATA_TRANSFORM.lower() + "-" + AWS_ENV
S3_DATA_TRANSFORM = S3_DATA_TRANSFORM.lower() + "-" + "prod" ## hardcoded for prod

S3_DATA_WORK = args["S3_DATA_WORK"] 
S3_DATA_WORK = S3_DATA_WORK.lower() + "-" + AWS_ENV

S3_DATA_CONSUME = args["S3_DATA_CONSUME"] 
# S3_DATA_CONSUME = S3_DATA_CONSUME.lower() + "-" + AWS_ENV
S3_DATA_CONSUME = S3_DATA_CONSUME.lower() + "-" + "prod"  ## hardcoded for prod for meter_event data

VAR_SNS_TOPIC = args["VAR_SNS_TOPIC"]  # ARN of SNS Topic
VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]

# VAR_AEP_EVENT_DT_FROM = args["VAR_AEP_EVENT_DT_FROM"]
# VAR_AEP_EVENT_DT_TO = args["VAR_AEP_EVENT_DT_TO"]
VAR_AEP_EVENT_DT_FROM = ''
VAR_AEP_EVENT_DT_TO = ''
##==============================================================================================##
## Define required paths

event_summ_regex_setup_path = "s3://aep-datalake-consume"+ "-" +  AWS_ENV + "/util/event_summ_regex_setup"   ## hardcoded for time being
end_device_event_consume_basePath  =  S3_DATA_CONSUME + "/util/events/uiq/end_device_event"

end_device_event_catg_stg_basePath = S3_DATA_WORK + "/util/events/uiq/end_device_event_catg_stg"  
end_device_event_errors_basePath = S3_DATA_WORK + "/util/events/uiq/end_device_event_errors"

meter_premise_basePath = S3_DATA_TRANSFORM + "/util/ods/meter_premise/hive_table"

#==============================================================================================##
##### retrieve last_run_dt_value from ssm parameter
ssm_last_run_dt_name=str(args['SSM_LAST_RUN_DT_NAME']).lower() + '_' + VAR_OPCO 
ssm_last_run_dt_resp = ssm_client.get_parameter(Name=ssm_last_run_dt_name, WithDecryption=False)
ssm_last_run_dt_val = ssm_last_run_dt_resp['Parameter']['Value']

var_today=date.today()
var_today = datetime.strptime(var_today.strftime('%Y-%m-%d'), '%Y-%m-%d') 

if ssm_last_run_dt_val.strip() == '':      

    exception_msg =f"Please provide {ssm_last_run_dt_name} parameter value for OPCO {VAR_OPCO} in yyyy-MM-dd~yyyy-MM-dd format"
    print(f'INFO: {exception_msg}')
    raise Exception(exception_msg)
    # os._exit(0)

else:
    ### Add one day to ssm_last_run_dt_val to get the new start date for current run
    # start_date = datetime.strptime(ssm_last_run_dt_val, '%Y-%m-%d')  + timedelta(days = 1)
    # start_date = start_date.strftime('%Y-%m-%d')
    # new_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days = 30)

    # if new_date >=var_today:       

    #     end_date = var_today - timedelta(days = 1)  
    #     end_date = end_date.strftime('%Y-%m-%d')

    #     # end_date = var_today.strftime('%Y-%m-%d')
    # else :
    #     end_date = new_date.strftime('%Y-%m-%d')

    start_date=ssm_last_run_dt_val.split('~')[0]
    end_date=ssm_last_run_dt_val.split('~')[1]
    print(start_date, end_date)

    VAR_AEP_EVENT_DT_FROM=start_date
    VAR_AEP_EVENT_DT_TO = end_date

print('VAR_AEP_EVENT_DT_FROM:',VAR_AEP_EVENT_DT_FROM)
print('VAR_AEP_EVENT_DT_TO:',VAR_AEP_EVENT_DT_TO)
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
end_device_event_consume_paths = []
for event_dt in VAR_AEP_EVENT_DATES:
    end_device_event_consume_paths = end_device_event_consume_paths + [ os.path.join( end_device_event_consume_basePath,f"aep_opco={VAR_OPCO}",f"aep_event_dt={event_dt}") ]

##==============================================================================================##
## check if all s3 paths are valid

s3_data_consume_bucket_name=S3_DATA_CONSUME[5:] 
all_paths = end_device_event_consume_paths
end_device_event_consume_paths = []
for path in all_paths:
    bucket_name, key = path.replace('s3://','').split('/',1)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key)
        if 'Contents' in response:
            end_device_event_consume_paths.append(path)
    except Exception as e:
        pass

# print('end_device_event_consume_paths:',str(end_device_event_consume_paths) )

##==============================================================================================##
## Get meter_premise_paths [OPCO wise]
co_cd_ownrs=getCompanyCode(VAR_OPCO)     
meter_premise_paths = [] 
for co_cd_ownr in co_cd_ownrs:
    meter_premise_paths = meter_premise_paths + [ os.path.join( meter_premise_basePath,f"co_cd_ownr={co_cd_ownr}") ]
    
print('DEBUG: meter_premise_paths:',str(meter_premise_paths) )
##==============================================================================================##
## Define File Structure -- end_device_event

end_device_event_schema=StructType([
StructField("issuertracking_id", StringType(), True),
StructField("issuer_id", StringType(), True),
StructField("serialnumber", StringType(), True),
StructField("enddeviceeventtypeid", StringType(), True),
StructField("aep_timezone_cd", StringType(), True),
StructField("valuesinterval", StringType(), True),
StructField("aep_devicecode", StringType(), True),
StructField("aep_mtr_pnt_nb", StringType(), True),
StructField("aep_tarf_pnt_nb", StringType(), True),
StructField("aep_premise_nb", StringType(), True),
StructField("aep_service_point", StringType(), True),
StructField("aep_bill_account_nb", StringType(), True),
StructField("reason", StringType(), True),
StructField("user_id", StringType(), True),
StructField("manufacturer_id", StringType(), True),
StructField("domain", StringType(), True),
StructField("eventoraction", StringType(), True),
StructField("sub_domain", StringType(), True),
StructField("event_type", StringType(), True),
StructField("aep_state", StringType(), True),
StructField("aep_area_cd", StringType(), True),
StructField("aep_sub_area_cd", StringType(), True),
StructField("longitude", StringType(), True),
StructField("latitude", StringType(), True),
StructField("aep_city", StringType(), True),
StructField("aep_zip", StringType(), True),
StructField("hdp_update_user", StringType(), True),
StructField("hdp_insert_dttm", TimestampType(), True),
StructField("hdp_update_dttm", TimestampType(), True),
StructField("aep_opco", StringType(), True),
StructField("aep_event_dt", StringType(), True)
])

end_device_event_df =  spark.read.schema(end_device_event_schema) \
.option("basePath", end_device_event_consume_basePath) \
.option("inferSchema", "false") \
.option("compression", "snappy") \
.format("orc") \
.load(path=end_device_event_consume_paths)

end_device_event_df.cache()
## Keep end_device_event_df for error records
end_device_event_raw_df=end_device_event_df
# end_device_event_raw_df=end_device_event_raw_df.drop("aep_opco","aep_event_dt") 

meter_premise_schema=StructType([
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

##==============================================================================================##
### Load default.meter_premise

meter_premise_df_raw =  spark.read.schema(meter_premise_schema) \
.option("basePath", meter_premise_basePath) \
.option("inferSchema", "false") \
.option("mergeSchema", "false") \
.option("delimiter", "\t") \
.format("csv") \
.load(path=meter_premise_paths)


##### Get meter count 
mp_xfmr_mtr_cnt=meter_premise_df_raw.groupBy('trsf_pole_nb').agg(F.countDistinct(meter_premise_df_raw.mfr_devc_ser_nbr).alias('xf_meter_cnt'))
mp_xfmr_mtr_cnt=mp_xfmr_mtr_cnt.withColumnRenamed('trsf_pole_nb', 'xtrsf_pole_nb')

meter_premise_df = meter_premise_df_raw.join(mp_xfmr_mtr_cnt, (meter_premise_df_raw.trsf_pole_nb == mp_xfmr_mtr_cnt.xtrsf_pole_nb), how="inner")
meter_premise_df = meter_premise_df.drop('xtrsf_pole_nb')

##==============================================================================================##
## Join meter event and meter premise data

meter_event_prem = end_device_event_df.join( meter_premise_df, (end_device_event_df.serialnumber == meter_premise_df.mfr_devc_ser_nbr), how="left_outer")\
    .select( \
    end_device_event_df.issuertracking_id, \
    end_device_event_df.serialnumber, \
    end_device_event_df.enddeviceeventtypeid, \
    end_device_event_df.valuesinterval, \
    end_device_event_df.aep_devicecode, \
    end_device_event_df.aep_mtr_pnt_nb, \
    end_device_event_df.aep_tarf_pnt_nb, \
    end_device_event_df.aep_premise_nb, \
    end_device_event_df.reason, \
    end_device_event_df.aep_state, \
    end_device_event_df.aep_area_cd, \
    end_device_event_df.aep_sub_area_cd, \
    end_device_event_df.longitude, \
    end_device_event_df.latitude, \
    end_device_event_df.aep_city, \
    end_device_event_df.aep_zip, \
    end_device_event_df.aep_opco, \
    end_device_event_df.aep_event_dt, \
    meter_premise_df.trsf_pole_nb, \
    meter_premise_df.circuit_nb, \
    meter_premise_df.circuit_nm, \
    meter_premise_df.station_nb, \
    meter_premise_df.station_nm, \
    meter_premise_df.xf_meter_cnt \
    )

# print('meter_event_prem count:',str(meter_event_prem.count()))
meter_event_prem.cache()

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
StructField("eff_dt",StringType(),True),
StructField("aws_update_dttm",StringType(),True)
])

regex_setup_df =  spark.read.schema(regex_setup_schema) \
.option("inferSchema", "false") \
.option("mergeSchema", "false") \
.option("delimiter", "~") \
.format("csv") \
.load(path=event_summ_regex_setup_path)


regex_setup_df=regex_setup_df.select("enddeviceeventtypeid","regex_search_pattern","regex_replacement_pattern","regex_id")
regex_setup_df=regex_setup_df.withColumn('regex_replacement_pattern',F.when(F.col('regex_replacement_pattern').isNull(),'').otherwise(F.col('regex_replacement_pattern'))).distinct().orderBy(F.col('enddeviceeventtypeid'),F.col('regex_id'))

grouped_df = regex_setup_df.groupBy("enddeviceeventtypeid").agg(F.collect_list(F.struct("regex_search_pattern","regex_replacement_pattern","regex_id")).alias("rows"))

regex_dict={}
regex_lst=[]
regex_row=[]
for row in grouped_df.collect():
	rows=row.rows
	for r in rows:
		for col_name in r.__fields__:
			regex_row.append(getattr(r,col_name))
		regex_lst.append(tuple(regex_row))
		regex_row=[]
	regex_dict[row.enddeviceeventtypeid]=regex_lst
	regex_lst=[]

###################### For testing 	
# regex_dict = dict((key,regex_dict[key]) for key in ['3.21.38.223'])
# print('regex_dict:',str(regex_dict))

##==============================================================================================##
## loop through regex_dict and derive curated_reason and regex_id

# df=spark.createDataFrame([],meter_event_prem.schema)
# df=df.withColumn("cur_reason_regex_id",F.lit(""))
# print('printing df.schema')
# df.printSchema()

res=[]
for event_type, pattern_list in regex_dict.items():
    event_rows_df = meter_event_prem.filter(F.col("enddeviceeventtypeid") == event_type)
    if event_rows_df.count() == 0:
         continue
    counter=0
    for pattern_tuple in pattern_list:        
          
        if counter == 0:   
            event_rows_df = event_rows_df.withColumn(
                "cur_reason_regex_id",F.when(F.regexp_extract(F.col('reason'), pattern_tuple[0],0) !="", 
                                    F.concat_ws("~", F.regexp_replace(F.col('reason'), pattern_tuple[0], pattern_tuple[1]) , F.lit(pattern_tuple[2]) ) ).otherwise("")            
            )
        else:
            event_rows_df = event_rows_df.withColumn(
                "cur_reason_regex_id",F.when(F.regexp_extract(F.col('reason'), pattern_tuple[0],0) !="" ,
                                            F.when( F.col("cur_reason_regex_id") !="",
                                                 F.concat_ws("^", F.col("cur_reason_regex_id") , F.concat_ws("~", F.regexp_replace(F.col('reason'), pattern_tuple[0], pattern_tuple[1]) , F.lit(pattern_tuple[2]) ) )
                                                   ).otherwise(F.concat_ws("~", F.regexp_replace(F.col('reason'), pattern_tuple[0], pattern_tuple[1]) , F.lit(pattern_tuple[2]) ))
                                            ).otherwise(F.col('cur_reason_regex_id'))
            )

        counter=counter+1

    ##==============================================================================================##
    ## select the required columns

    event_rows_df=event_rows_df.select( \
    "issuertracking_id", \
    "serialnumber", \
    "enddeviceeventtypeid", \
    "valuesinterval", \
    "aep_devicecode", \
    "aep_mtr_pnt_nb", \
    "aep_tarf_pnt_nb", \
    "aep_premise_nb", \
    "reason", \
    "aep_state", \
    "aep_area_cd", \
    "aep_sub_area_cd", \
    "longitude", \
    "latitude", \
    "aep_city", \
    "aep_zip", \
    "aep_opco", \
    "aep_event_dt", \
    "trsf_pole_nb", \
    "circuit_nb", \
    "circuit_nm", \
    "station_nb", \
    "station_nm", \
    "xf_meter_cnt", \
    "cur_reason_regex_id" \
    )

    # df = df.union(event_rows_df)
    res.append(event_rows_df)
##==============================================================================================##
##  rename the unioned df back to  event_rows_df

# event_rows_df = df
# print('printing event_rows_df.schema')
# event_rows_df.printSchema()   
# ### check if it is empty dataset of not
# event_rows_df_cnt=event_rows_df.count()
# print('event_rows_df.count:', str(event_rows_df_cnt))

# if event_rows_df_cnt == 0: 
#     print('Exiting the job process as it could not find any dataset on which regex to be applied..')
#     os._exit(0)

if len(res) == 0: 
    print('INFO: Exiting the job process as it could not find any dataset on which regex to be applied..')
    os._exit(0)
else :    
    event_rows_df=unionAll(*res)


# event_rows_df=event_rows_df.withColumn("cur_reason_regex_id_split_len",F.size(F.split("cur_reason_regex_id","\\^")))
# event_rows_df=event_rows_df.withColumn("is_mult_regex_applied",F.when(F.col("cur_reason_regex_id_split_len") == 1, F.lit(0) ).otherwise(F.lit(1)) )
event_rows_df=event_rows_df.withColumn("no_regex_applied",F.when(F.col("cur_reason_regex_id") == "", F.lit(1) ).otherwise(F.lit(0)) )


##==============================================================================================##
##  Get new records having new eventtypeid which are not there in regex_setup table

end_device_event_raw_df_temp=end_device_event_raw_df.alias("end_device_event_raw_df")

join_cond=[end_device_event_raw_df_temp.issuertracking_id == event_rows_df.issuertracking_id,end_device_event_raw_df_temp.serialnumber == event_rows_df.serialnumber,end_device_event_raw_df_temp.valuesinterval == event_rows_df.valuesinterval,end_device_event_raw_df_temp.enddeviceeventtypeid == event_rows_df.enddeviceeventtypeid ]

new_eventtype_records_df =  end_device_event_raw_df_temp.join(event_rows_df,on=join_cond,how="left_anti") \
    .select("end_device_event_raw_df.*" )

##==============================================================================================##
##  run_dt
run_dt =F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss") 

new_eventtype_records_df=new_eventtype_records_df \
    .withColumn("hist_or_incr",F.lit("hist") ) \
    .withColumn("error_descr", F.concat_ws('|', F.lit("new enddeviceeventtypeid"),F.col('enddeviceeventtypeid') ) ) \
    .withColumn("run_dt", F.lit(run_dt) )

##==============================================================================================##
#### create dataframe where no regex got applied

no_regex_applied_df = event_rows_df.filter(F.col("no_regex_applied") == 1 )

##==============================================================================================##
##  Get error records having no regex being applied on eventtypeids by joining with end_device_event_df

no_regex_join_cond=[end_device_event_raw_df_temp.issuertracking_id == no_regex_applied_df.issuertracking_id,end_device_event_raw_df_temp.serialnumber == no_regex_applied_df.serialnumber,end_device_event_raw_df_temp.valuesinterval == no_regex_applied_df.valuesinterval,end_device_event_raw_df_temp.enddeviceeventtypeid == no_regex_applied_df.enddeviceeventtypeid ]

error_events_df = no_regex_applied_df.join(end_device_event_raw_df_temp, on = no_regex_join_cond, how="inner") \
    .select("end_device_event_raw_df.*" )

error_events_df = error_events_df.withColumn("hist_or_incr", F.lit("hist") ) \
    .withColumn("error_descr",  F.lit("no_regex_applied") ) \
    .withColumn("run_dt", F.lit(run_dt) )
    # .withColumn("run_dt", F.date_format( F.to_date( F.col('aep_event_dt') ), "yyyyMMdd_HHmmss") )

##==============================================================================================##
##  Get both  error dataframes('no regex applied' and 'new enddeviceeventtypeid')

error_events_df = error_events_df.union(new_eventtype_records_df)

##==============================================================================================##
##  select and write to error table

error_events_df = error_events_df.select( \
  'issuertracking_id' , \
  'issuer_id' , \
  'serialnumber' , \
  'enddeviceeventtypeid' , \
  'aep_timezone_cd' , \
  'valuesinterval' , \
  'aep_devicecode' , \
  'aep_mtr_pnt_nb' , \
  'aep_tarf_pnt_nb' , \
  'aep_premise_nb' , \
  'aep_service_point' , \
  'aep_bill_account_nb' , \
  'reason' , \
  'user_id' , \
  'manufacturer_id' , \
  'domain' , \
  'eventoraction' , \
  'sub_domain' , \
  'event_type' , \
  'aep_state' , \
  'aep_area_cd' , \
  'aep_sub_area_cd' , \
  'longitude' , \
  'latitude' , \
  'aep_city' , \
  'aep_zip' , \
  'hdp_update_user' , \
  'hdp_insert_dttm' , \
  'hdp_update_dttm' ,\
  'hist_or_incr' ,\
  'error_descr', \
  'run_dt'  , \
  'aep_opco'  , \
  'aep_event_dt' \
 )

# print('showing some rec from error_events_df')
# error_events_df.show(2,truncate=False)

# print('DEBUG: error_events_df printSchema')
# error_events_df.printSchema()
error_events_df = error_events_df.repartition("run_dt","aep_opco", "aep_event_dt") 
error_events_df.write.mode("overwrite") \
.partitionBy("run_dt","aep_opco", "aep_event_dt") \
.format("orc") \
.option("compression", "SNAPPY") \
.save(end_device_event_errors_basePath)

##==============================================================================================##
## MSCK
print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table stg_meterevents.end_device_event_errors")

##==============================================================================================##
## configure mail regarding error event details

error_enddeviceeventtypeid_df=error_events_df.select("enddeviceeventtypeid").distinct()

error_enddeviceeventtypeid_df_cnt = error_enddeviceeventtypeid_df.count()

if  error_enddeviceeventtypeid_df_cnt > 0:
    # error_enddeviceeventtypeid_df.show()
    # lst_enddeviceeventtypeid=error_enddeviceeventtypeid_df.select("enddeviceeventtypeid").rdd.flatMap(lambda x:x).collect()
    # lst_string=','.join(map(str,lst_enddeviceeventtypeid))

    # msg_title=f"GlueJob: end-device-event-catg-hist-laod found some error events"
    # print(msg_title)
    # msg_body=f"## GlueJob: end-device-event-catg-hist-laod found some error events. Please check stg_meterevents.end_device_event_errors table for more info."
    # print(msg_body)
    # snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
    print("DEBUG: distinct count of enddeviceeventtypeid in error table for this run: "  + str(error_enddeviceeventtypeid_df_cnt))

##==============================================================================================##
#### preapre curated_reason data

event_rows_df = event_rows_df.filter(F.col("no_regex_applied") == 0 )
event_rows_df = event_rows_df.withColumn("curated_reason", F.split(F.split("cur_reason_regex_id","\\^")[0],"~").getItem(0) )
event_rows_df = event_rows_df.withColumn("regex_id", F.split(F.split("cur_reason_regex_id","\\^")[0],"~").getItem(1) )

curated_reason_df=event_rows_df \
    .withColumn('aws_update_dttm', F.lit(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))) 

curated_reason_df = curated_reason_df.select( \
    "issuertracking_id", \
    "serialnumber", \
    "enddeviceeventtypeid", \
    "valuesinterval", \
    "aep_devicecode", \
    "aep_mtr_pnt_nb", \
    "aep_tarf_pnt_nb", \
    "aep_premise_nb", \
    "aep_state", \
    "aep_area_cd", \
    "aep_sub_area_cd", \
    "longitude", \
    "latitude", \
    "aep_city", \
    "aep_zip", \
    "trsf_pole_nb", \
    "circuit_nb", \
    "circuit_nm", \
    "station_nb", \
    "station_nm", \
    "xf_meter_cnt", \
    "reason", \
    "curated_reason", \
    "regex_id", \
    "aws_update_dttm", \
    "aep_opco", \
    "aep_event_dt" \
    )

print("DEBUG: Final DF Ready - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
curated_reason_df.printSchema()

### curated_reason_df = curated_reason_df.repartition('aep_opco', 'aep_event_dt').sortWithinPartitions( ['trsf_pole_nb', 'serialnumber'] )

curated_reason_df = curated_reason_df.repartition('aep_opco', 'aep_event_dt')
# print('INFO: curated_reason_df count:',str(curated_reason_df.count()))

curated_reason_df.write.mode("overwrite") \
.partitionBy("aep_opco", "aep_event_dt") \
.option("parquet.bloom.filter.enabled#aep_premise_nb", "true")\
.option("parquet.bloom.filter.enabled#serialnumber", "true")\
.option("parquet.bloom.filter.enabled#trsf_pole_nb", "true")\
.option("parquet.bloom.filter.enabled#enddeviceeventtypeid", "true")\
.option("parquet.bloom.filter.expected.ndv#aep_premise_nb", "5000000")\
.option("parquet.bloom.filter.expected.ndv#serialnumber", "5000000")\
.option("parquet.bloom.filter.expected.ndv#trsf_pole_nb", "2000000")\
.option("parquet.bloom.filter.expected.ndv#enddeviceeventtypeid", "5000000")\
.option("parquet.enable.dictionary", "true")\
.format("parquet") \
.option("compression", "SNAPPY") \
.option("spark.sql.files.maxRecordsPerFile", 250000) \
.save(end_device_event_catg_stg_basePath)

print("INFO: Done with Writing out the DF  - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
print("INFO: Partition " + end_device_event_catg_stg_basePath + " successfully loaded.")

##==============================================================================================##
## MSCK
print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table stg_meterevents.end_device_event_catg_stg") 

print("INFO: stg_meterevents.end_device_event_catg_stg successfully loaded.")

##==============================================================================================##
## if everything is ok, then set run_dt_for_next_run value for next run

# ssm_last_run_dt_resp = ssm_client.put_parameter(Name=ssm_last_run_dt_name, Value= VAR_AEP_EVENT_DT_TO , Type='String', Overwrite=True )


## Stop the Context
job.commit()
sc.stop()

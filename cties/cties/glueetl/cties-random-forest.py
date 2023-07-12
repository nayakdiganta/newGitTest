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
##/**   V0.1       Deepika                  11/14/2022      Random Forest Model in Spark       **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/

## Import Libraries -- Mostly Boilerplate
## These Libraries would change based on AWS Glue ETL Setup - Be Minimalistic
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf

from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, StructType, StructField, DoubleType, FloatType, BinaryType,LongType,ArrayType
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit,unix_timestamp
from pyspark.sql.functions import udf,pandas_udf
import json
import os
import sys
import boto3
from botocore.exceptions import ClientError
import base64
import logging
import uuid
# from datetime import datetime, date
from datetime import datetime, date, timedelta
from time import time
t = time()
import pickle
from io import BytesIO
import joblib

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark import SparkFiles
import traceback
##=========RF Model Libraries=================##
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.metrics import roc_curve, precision_recall_curve, auc, make_scorer, recall_score, accuracy_score, precision_score, confusion_matrix
from sklearn.metrics import f1_score,accuracy_score

##==============================================================================================##
## Collect Run Parameters 

# args = getResolvedOptions(sys.argv, ['JOB_NAME', 'AWS_ENV', 'VAR_OPCO','S3_CTIES_CONSUME','VAR_JOBLIB_FILE_PATH','SSM_RUN_INTERVAL_NAME','SSM_RUN_TYPE_NAME','SSM_LAST_RUN_AEP_USAGE_DT_TO_NAME','VAR_SNS_TOPIC'])

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'AWS_ENV', 'VAR_OPCO','S3_CTIES_CONSUME','VAR_JOBLIB_FILE_PATH','SSM_RUN_INTERVAL_NAME','SSM_LAST_RUN_AEP_USAGE_DT_TO_NAME','VAR_SNS_TOPIC'])

# get boto3 clients
sns_client=boto3.client('sns')
ssm_client = boto3.client('ssm')
glue_client = boto3.client('glue')
s3_resource = boto3.resource('s3')

##==============================================================================================##
## Helper Functions

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


fnLog("INFO", f"args passed to job: {str(args)}")

##==============================================================================================##
## Extract job parameters 

AWS_ENV = args['AWS_ENV']
aws_env=AWS_ENV.lower()

VAR_OPCO = args['VAR_OPCO'] 
var_opco=VAR_OPCO.lower()

S3_CTIES_CONSUME = args['S3_CTIES_CONSUME']
s3_cties_consume = S3_CTIES_CONSUME.lower() + "-" + aws_env
print('DEBUG: s3_cties_consume',s3_cties_consume)

VAR_SNS_TOPIC = args['VAR_SNS_TOPIC'] ## ARN of SNS Topic
VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]
print('DEBUG: VAR_SNS_TOPIC_SUCCESS: ',VAR_SNS_TOPIC_SUCCESS)
print('DEBUG: VAR_SNS_TOPIC_FAILURE: ',VAR_SNS_TOPIC_FAILURE)

#bucket_name = "aep-datalake-consume-dev"
#bucket_key = "cties/Training_Data/Rf_tiesV1.joblib"
# Bucket_name = args['VAR_Bucket_name']
# bucket_name = Bucket_name + "-" + aws_env
bucket_name = s3_cties_consume[5:]

var_joblib_file_path = args['VAR_JOBLIB_FILE_PATH']

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


print('DEBUG: VAR_AEP_USAGE_DT_FROM from ssm : ',VAR_AEP_USAGE_DT_FROM)
print('DEBUG: VAR_AEP_USAGE_DT_TO from ssm : ',VAR_AEP_USAGE_DT_TO)

#######Hardcoded for the timebeing
# VAR_AEP_USAGE_DT_FROM='2023-01-01'
# VAR_AEP_USAGE_DT_TO='2023-01-15'
# print('DEBUG: VAR_AEP_USAGE_DT_FROM hardcode : ',VAR_AEP_USAGE_DT_FROM)
# print('DEBUG: VAR_AEP_USAGE_DT_TO hardcode: ',VAR_AEP_USAGE_DT_TO)

##==============================================================================================##
## Define required data paths 
cties_voltage_summary_basePath=s3_cties_consume + "/cties/meter_xfmr_voltg_summ/"   ## need to be modified
#cties_voltage_summary_basePath=s3_cties_consume + "/cties/meter_xfmr_voltg_summ/" 
print('cties_voltage_summary_basePath',cties_voltage_summary_basePath)
cties_voltage_summary_path=cties_voltage_summary_basePath +"aep_opco="+var_opco
cties_RF_summary_basePath=s3_cties_consume + "/cties/meter_xfmr_voltg_rf/"
cties_RF_summary_path=cties_RF_summary_basePath+"aep_opco="+var_opco

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
# job.commit()
conf=SparkConf()

current_user1 = sqlContext._sc.sparkUser()

##==============================================================================================##
## User defined functions

@udf('integer')
def predict_udf(*cols):
	return int(broadcast_model.value.predict((cols,)))

@pandas_udf('integer')
def predict_pandas_udf(*cols):
	data=pd.concat(cols,axis=1)
	return pd.Series(broadcast_model.value.predict(data))

##==============================================================================================##
## Initialize the Run Parameters and Such

# try:
         
cns_mtr_vltg_schema=StructType([\
    StructField('aep_premise_nb',StringType(),True),\
    StructField('serialnumber',StringType(),True),\
    StructField('trsf_pole_nb',StringType(),True),\
    StructField('trsf_mount_cd',StringType(),True),\
    StructField('gis_circuit_nb',StringType(),True),\
    StructField('gis_circuit_nm',StringType(),True),\
    StructField('gis_station_nb',StringType(),True),\
    StructField('gis_station_nm',StringType(),True),\
    StructField('company_cd',StringType(),True),\
    StructField('srvc_entn_cd',StringType(),True),\
    StructField('distance_feets',DoubleType(),True),\
    StructField('xf_meter_cnt',LongType(),True),\
    StructField('aep_derived_uom',StringType(),True),\
    StructField('aep_raw_uom',StringType(),True),\
    StructField('aep_channel_id',StringType(),True),\
    StructField('gis_mapping_asof_dt',StringType(),True),\
    StructField('read_array',ArrayType(FloatType()),True),\
    StructField('xf_read_array',ArrayType(FloatType()),True),\
    StructField('xf_read_array_whole',ArrayType(FloatType()),True),\
    StructField('eucl_dist',FloatType(),True),\
    StructField('cos_sim',FloatType(),True),\
    StructField('shape_mismatch_cnt',IntegerType(),True),\
    StructField('mean_eucl_dist',DoubleType(),True),\
    StructField('mean_shape_mismatch_cnt',DoubleType(),True),\
    StructField('stddev_eucl_dist',DoubleType(),True),\
    StructField('stddev_shape_mismatch_cnt',DoubleType(),True),\
    StructField('z_score_eucl_dist',DoubleType(),True),\
    StructField('z_score_shape_mismatch_cnt',DoubleType(),True),\
    StructField('median_eucl_dist',FloatType(),True),\
    StructField('median_shape_mismatch_cnt',IntegerType(),True),\
    StructField('median_z_score_eucl_dist',DoubleType(),True),\
    StructField('aws_update_dttm',StringType(),True),\
    StructField('run_control_id',LongType(),True),\
    StructField('aep_opco',StringType(),True),\
    StructField('aep_usage_dt',DateType(),True)\
    ])

##==============================================================================================##
## Loop through the dates and prepare the Partition List

VAR_AEP_USAGE_DATES = pd.date_range(start=VAR_AEP_USAGE_DT_FROM, end=VAR_AEP_USAGE_DT_TO, freq='D').strftime("%Y-%m-%d")
cties_voltage_summary_paths=[]
for usage_dt in VAR_AEP_USAGE_DATES:
    cties_voltage_summary_paths = cties_voltage_summary_paths + [ os.path.join(cties_voltage_summary_basePath, f"aep_opco={var_opco}", f"aep_usage_dt={usage_dt}") ]

print("\n\n")
print("DEBUG: VAR_AEP_USAGE_DATES: <"+str(VAR_AEP_USAGE_DATES)+">")
print("DEBUG: cties_voltage_summary_basePath: <"+str(cties_voltage_summary_basePath)+">")
print("DEBUG: cties_voltage_summary_paths: <"+str(cties_voltage_summary_paths)+">")
print("\n\n")

##==============================================================================================##  
cns_mtr_vltg_mp=spark.read.schema(cns_mtr_vltg_schema) \
    .option("basePath", cties_voltage_summary_basePath) \
    .option("inferSchema", "false") \
    .option("compression", "snappy") \
    .option("mergeSchema", "false") \
    .format("parquet") \
    .load(path=cties_voltage_summary_paths)
    
cns_mtr_vltg_mp.printSchema()
print("DEBUG: Consume table data= "  + str(cns_mtr_vltg_mp.count()))

    
##===========================Unique Meter ID Extraction============================================================##

# mtr_vltg_uniq = cns_mtr_vltg_mp.filter(col("aep_opco")==VAR_OPCO)
# mtr_vltg_uniq = cns_mtr_vltg_mp.filter(col("run_control_id")==VAR_RUN_ID)

mtr_vltg_uniq = cns_mtr_vltg_mp.select("aep_premise_nb","serialnumber","trsf_pole_nb","trsf_mount_cd","gis_circuit_nb", \
                "gis_circuit_nm","gis_station_nb","gis_station_nm","company_cd","srvc_entn_cd","distance_feets","xf_meter_cnt", \
                "median_eucl_dist","median_shape_mismatch_cnt","median_z_score_eucl_dist","run_control_id","aep_opco").distinct()


print("DEBUG: Unique Meter Transformer mp - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

print("DEBUG: Unique Meter - count= "  + str(mtr_vltg_uniq.count()))
mtr_vltg_uniq.show(2)

mtr = mtr_vltg_uniq.where(col("median_z_score_eucl_dist").isNotNull())

print("DEBUG: Unique Meter - count without null values = "  + str(mtr.count()))
mtr.show(2)

##=========================Loading Data from Joblib=======================##

with BytesIO() as data:
    s3_resource.Bucket(bucket_name).download_fileobj(var_joblib_file_path,data)
    data.seek(0)
    model_rf = joblib.load(data)
    
# print("type" , type(model_rf))


broadcast_model = sc.broadcast(model_rf)

feature_cols=['distance_feets','median_eucl_dist','median_z_score_eucl_dist','median_shape_mismatch_cnt']


mtr_pd = mtr.toPandas()

mtr_pd[['pred_0', 'pred_1']] = model_rf.predict_proba(mtr_pd[['distance_feets','median_eucl_dist','median_z_score_eucl_dist','median_shape_mismatch_cnt']])    

# print("Pandas op")
# print(mtr_pd)

mtr_sk_prob = spark.createDataFrame(mtr_pd)

mtr_sk_prob.show(5)

mtr_op = mtr\
    .withColumn('y_pred', predict_pandas_udf(*feature_cols)) 

print("The final spark op")
mtr_op.show(2)
mtr_prob_pred =mtr_op.alias("pred") \
                .join(mtr_sk_prob.alias("prb"),['serialnumber','trsf_pole_nb'],how = "inner") \
                .select('pred.aep_premise_nb','pred.serialnumber','pred.trsf_pole_nb','pred.trsf_mount_cd','pred.gis_circuit_nb','pred.gis_circuit_nm',\
                'pred.gis_station_nb','pred.gis_station_nm','pred.company_cd','pred.srvc_entn_cd','pred.distance_feets','pred.xf_meter_cnt','pred.median_eucl_dist',\
                'pred.median_shape_mismatch_cnt','pred.median_z_score_eucl_dist','pred.y_pred','prb.pred_0','prb.pred_1','pred.run_control_id')

print("Alloutput")
# mtr_prob_pred.show(2)
# mtr_prob_pred.printSchema()


final_df = mtr_prob_pred \
    .withColumn('aws_update_dttm', lit(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))) 
    
final_rf = final_df \
                .withColumn("aep_opco",lit(var_opco)) \
                .withColumn("aep_run_dt",lit(datetime.today().strftime("%Y-%m-%d"))) \
                .select('aep_premise_nb','serialnumber','trsf_pole_nb','trsf_mount_cd','gis_circuit_nb','gis_circuit_nm',\
                'gis_station_nb','gis_station_nm','company_cd','srvc_entn_cd','distance_feets','xf_meter_cnt','median_eucl_dist',\
                'median_shape_mismatch_cnt','median_z_score_eucl_dist','y_pred','pred_0','pred_1',\
                'run_control_id','aws_update_dttm','aep_opco','aep_run_dt').distinct()
# print("type" , type(final_rf))
final_rf.printSchema()
final_rf.show(5)

##==============================================================================================##
## Write the Data Out to Target Folder

final_rf = final_rf.repartition('aep_opco', 'aep_run_dt')
print("DEBUG: Done with Repartition the DF  - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

print("DEBUG: Starting To Write the Frame to Final RF Table - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

final_rf.write.mode("overwrite") \
    .partitionBy("aep_opco", "aep_run_dt") \
    .option("compression", "SNAPPY") \
    .option("spark.sql.files.maxRecordsPerFile", 1000000) \
    .format("parquet") \
    .save(cties_RF_summary_basePath)
    
print("DEBUG: Done with Writing to Random Forest Mapping Folder - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table cties.meter_xfmr_voltg_rf") 

##==============================================================================================##    
##update the ssm parameter value for next run
print("DEBUG: updating ssm parameter values for next run..")
ssm_last_run_aep_usage_dt_to_resp = ssm_client.put_parameter(Name=ssm_last_run_aep_usage_dt_to_name, Value= VAR_AEP_USAGE_DT_TO , Type='String', Overwrite=True )
# ssm_run_type_resp = ssm_client.put_parameter(Name=ssm_run_type_name, Value='s' , Type='String', Overwrite=True )

job.commit()
sc.stop()
# except Exception as e:
#     print(e)
#     msg_title=f"GlueJob: cties-random-forest failed"
#     print(msg_title)
#     msg_body=f"## GlueJob: cties-random-forest.py failed while processing for OPCO: <{VAR_OPCO}> for date range {VAR_AEP_USAGE_DT_FROM} and {VAR_AEP_USAGE_DT_TO}"
#     print(msg_body)
#     snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
#     raise e   
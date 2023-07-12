##/**------------------------------------------------------------------------------------------**/
##/**          AMERICAN ELECTRIC POWER - CTIES meter_premise Data                              **/
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
##/**   V0.1      Diganta        02/20/2023  script for cties-meter-premise-src-stg            **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/
## s3://aep-datalake-apps-dev/hdpapp/cties/glueetl/cties-meter-premise-src-stg.py              **/
##==============================================================================================##
## Import Libraries 

from pyspark.context import SparkContext, SparkConf 
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
#from pyspark_llap.sql.session import HiveWarehouseSession # <AWS> EMR Related change

from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType 
from pyspark.sql.functions import isnull, col, lit, when, to_date, count, array, udf,unix_timestamp
from pyspark.sql.types import StructType, StructField
import pyspark.sql.functions as func
import sys
import os
import subprocess
import base64
import json
from pyspark.sql.functions import unix_timestamp, date_format, current_timestamp, current_date,trim
from datetime import datetime, timedelta
from pyspark import SparkFiles
from requests import Session
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import base64
from botocore.exceptions import ClientError
import logging
import traceback
from datetime import date, datetime, timedelta


import logging
import time
t = time.time()
args = getResolvedOptions(sys.argv, ['JOB_NAME','AWS_ENV', 'S3_DATA_WORK','S3_DATA_CONSUME', 'VAR_SNS_TOPIC','VAR_SECRET_NAME','VAR_OPCO'])
spark_appname = args['JOB_NAME']
AWS_ENV = args['AWS_ENV']
AWS_ENV = AWS_ENV.lower()

VAR_OPCO = args['VAR_OPCO'] ## "pso" 
VAR_OPCO = VAR_OPCO.lower()

# secret_name = "/aep/datalake/"+AWS_ENV+"/mv90/db/utl_login"
secret_name = args['VAR_SECRET_NAME'].lower()

S3_DATA_WORK = args['S3_DATA_WORK']
s3_data_work = S3_DATA_WORK.lower() + "-" + AWS_ENV
print('s3_data_work:',s3_data_work)

S3_DATA_CONSUME = args['S3_DATA_CONSUME']

VAR_SNS_TOPIC = args['VAR_SNS_TOPIC'] ## ARN of SNS Topic
VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]
# print('DEBUG: VAR_SNS_TOPIC_SUCCESS:',VAR_SNS_TOPIC_SUCCESS)
# print('DEBUG: VAR_SNS_TOPIC_FAILURE',VAR_SNS_TOPIC_FAILURE)

##==============================================================================================##
## Meter_premise S3 path for table in Athena

meter_premise_basePath = s3_data_work + "/util/cties/meter_premise_stg/"
##==============================================================================================##
###.config("spark.sql.session.timeZone", "EST5EDT")\ removed this from sparksession as it was giving different result for some date field 'FIRST_IN_SRVC_DT'
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
.config("spark.sql.debug.maxToStringFields", 1000)\
.config("spark.sql.execution.arrow.pyspark.enabled", "true")\
.config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")\
.enableHiveSupport().getOrCreate()

spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInRead', 'CORRECTED')

sc = spark.sparkContext
sqlContext = SQLContext(sc, spark)

glueContext = GlueContext(sc.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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

## initiate s3 resource through boto3
s3_resource = boto3.resource('s3') 
s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')
sns_client = boto3.client("sns")
glue_client = boto3.client('glue')

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

def getCompanyCode(aep_opco):
    co_cd_ownr = {
        "ap": ['01', '02', '06'],
        "im": ['04'],
        "kpc": ['03'],
        "oh": ['07', '10'],
        "pso": ['95'],
        "swp": ['96'],
        "tx": ['94', '97'],
     }
    co_cd_ownr_filter = co_cd_ownr.get(aep_opco, "['XX']")  
    return co_cd_ownr_filter

def get_secret(secret_name):
    #secret_name_list = sys.argv[2].split()
    # secret_name = "/aep/datalake/"+AWS_ENV+"/mv90/db/utl_login"
    # secret_name = "/aep/datalake/dev/mv90/db/utl_login"
    print("DEBUG: " + secret_name)
    region_name = "us-east-1"
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        else:
            ## Not Sure about this Exception - It is not Handled
            ## Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret

##==============================================================================================##
######## Get secret mgr details
# try:
secret = get_secret(secret_name)
secret_json = json.loads(secret)

VAR_HOST=secret_json["host"]
VAR_PORT=secret_json["port"]
VAR_SID=secret_json["dbname"]
VAR_USER=secret_json["username"]
VAR_PASS=secret_json["password"]

print('DEBUG Checkpoint --> ' + VAR_HOST + '-' + VAR_PORT + '-' + VAR_SID + '-' + VAR_USER)
    
ConnectString = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=" + VAR_HOST + ")(PORT=" + VAR_PORT + ")))(CONNECT_DATA=(SERVICE_NAME=" + VAR_SID + ")))"
DBDriver = "oracle.jdbc.driver.OracleDriver"

##==============================================================================================##
######## Columns to Select

var_cols_to_select="PREM_NB,CO_CD_OWNR,STATE_CD,SRVC_PNT_NM,MTR_PNT_NB,TARF_PNT_NB,PREMISE_ID,CURR_BILL_ACCT_NB,CURR_BILL_ACCT_ID"
var_cols_to_select=var_cols_to_select + "," + "CURR_RVN_CLS_CD,RVN_CLS_CD_DESC,MFR_DEVC_SER_NBR,MTR_STAT_CD,MTR_STAT_CD_DESC,MTR_POINT_LOCATION,DEVC_STAT_CD"
var_cols_to_select=var_cols_to_select + "," + "DEVC_STAT_CD_DESC,DEVC_CD,DEVC_CD_DESC,VINTAGE_YEAR,FIRST_IN_SRVC_DT,PHYS_INST_DT,RMVL_TS,DIAL_CNST,BILL_CNST"
var_cols_to_select=var_cols_to_select + "," + "TECHNOLOGY_TX,TECHNOLOGY_DESC,COMM_CD,COMM_DESC,TYPE_OF_SRVC_CD,TYPE_OF_SRVC_CD_DESC,TYPE_SRVC_CD,TYPE_SRVC_CD_DESC"
var_cols_to_select=var_cols_to_select + "," + "DISTRIBUTED_GENERATION_FLAG,INTERVAL_DATA,INTRVL_DATA_USE_CD,INTRVL_DATA_USE_CD_DESC,MTR_KIND_CDS"
var_cols_to_select=var_cols_to_select + "," + "INST_TS,BILL_FL,MFR_CD,MFR_CD_DESC,LAST_FLD_TEST_DATE,PGM_ID_NM"

##==============================================================================================##
## build opco details for where condition

var_opco_for_where_cond=' where co_cd_ownr '
co_cd_ownr=getCompanyCode(VAR_OPCO)

if len(co_cd_ownr) == 1:
    var_opco_for_where_cond=var_opco_for_where_cond + f"='{str(co_cd_ownr[0])}'"
else:
    var_opco_for_where_cond=var_opco_for_where_cond + f"in{tuple(co_cd_ownr)}"
print("DEBUG: var_opco_for_where_cond: ",var_opco_for_where_cond)

##==============================================================================================##
## Get meter premise data from ansersds through jdbc

df_meter_premise=sqlContext.read.format("jdbc") \
.option("url",ConnectString).option("driver",DBDriver) \
.option("fetchsize",50000) \
.option("dbtable","(SELECT " + var_cols_to_select + " FROM ANSERSDS.METER_PREMISE_VW" + var_opco_for_where_cond + " )" ) \
.option("user",VAR_USER).option("password",VAR_PASS) \
.load()

##==============================================================================================##
## Transform the required cols accordingly 

df_meter_premise=df_meter_premise.withColumn("CURR_RVN_CLS_CD",col("CURR_RVN_CLS_CD").cast("double"))\
.withColumn("FIRST_IN_SRVC_DT",when( col("FIRST_IN_SRVC_DT").cast("date") <='1900-01-01', to_date(lit('1900-01-01'), 'yyyy-MM-dd')).otherwise(col("FIRST_IN_SRVC_DT").cast("date"))  )\
.withColumn("PHYS_INST_DT",when( col("PHYS_INST_DT").cast("date") <='1900-01-01', to_date(lit('1900-01-01'), 'yyyy-MM-dd')).otherwise(col("PHYS_INST_DT").cast("date"))  )\
.withColumn("RMVL_TS",when( col("RMVL_TS").cast("date") <='1900-01-01', to_date(lit('1900-01-01'), 'yyyy-MM-dd')).otherwise(col("RMVL_TS").cast("date"))  )\
.withColumn("DIAL_CNST",col("DIAL_CNST").cast("double"))\
.withColumn("BILL_CNST",col("BILL_CNST").cast("double"))

##==============================================================================================##
## add aep_opco

df_meter_premise=df_meter_premise.withColumn('aep_opco', lit(VAR_OPCO) )

df_meter_premise = df_meter_premise\
    .withColumn('aws_update_dttm', lit(datetime.today().strftime("%Y-%m-%d %H:%M:%S")))
    
df_meter_premise=df_meter_premise.repartition("aep_opco")

df_meter_premise.printSchema()    

##==============================================================================================##
##Load the data in staging table

df_meter_premise.write.mode("overwrite") \
    .partitionBy("aep_opco") \
    .option("compression", "SNAPPY") \
    .option("spark.sql.files.maxRecordsPerFile", 1000000) \
    .format("parquet") \
    .save(meter_premise_basePath)


print(f"DEBUG: Done with Writing to {meter_premise_basePath} Folder - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

##==============================================================================================##
## Will need MSCK after Spark is done.

print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table stg_cties.meter_premise_stg")

fnLog("INFO", " Partition " + meter_premise_basePath + " successfully loaded.")
fnLog("INFO", " Load succeeded in " + str(round(time.time() - t,2)) + " seconds.")

##==============================================================================================##
## Start cties-meter-xfmr-map-stg-consume job

# ## Get worker_type and worker_node params from ssm and Glue kickoff
# worker_type_ssm=f"/aep/analytics/hdp/{AWS_ENV}/cties/parms/worker_type"            
# worker_type_response = ssm_client.get_parameter(Name=worker_type_ssm, WithDecryption=False)
# worker_type = worker_type_response['Parameter']['Value']
# # worker_type = "G.2X"
# worker_node_ssm=f"/aep/analytics/hdp/{AWS_ENV}/cties/parms/map_stg_cons_worker_node"
# worker_node_response = ssm_client.get_parameter(Name=worker_node_ssm, WithDecryption=False)
# worker_node = int(worker_node_response['Parameter']['Value'])
# # worker_node = 5
# print(f"DEBUG: - worker_type: {worker_type}")
# print(f"DEBUG: - worker_node: {worker_node}")

# glueJobName="cties-meter-xfmr-map-stg-consume"
# print(f"DEBUG: - calling glueJobName: {glueJobName}")

# callArguments = {
#     '--AWS_ENV': AWS_ENV,
#     '--VAR_OPCO': VAR_OPCO, 
#     '--S3_DATA_WORK': S3_DATA_WORK,
#     '--S3_DATA_CONSUME': S3_DATA_CONSUME,    
#     '--VAR_SNS_TOPIC': VAR_SNS_TOPIC
#     }
# response = glue_client.start_job_run(JobName = glueJobName, WorkerType=worker_type, NumberOfWorkers=worker_node, Arguments=callArguments)
# # print(response)
# fnLog("INFO",  f"{glueJobName} started and runId is : {str(response)}")

## Stop the Context
job.commit()
sc.stop()
# except Exception as e:
#     print(e)
#     msg_title=f"GlueJob: cties-meter-premise-src-stg failed"
#     print(msg_title)
#     msg_body=f"## GlueJob: cties-meter-premise-src-stg.py failed while processing for OPCO: <{VAR_OPCO}>"
#     print(msg_body)
#     snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
#     raise e
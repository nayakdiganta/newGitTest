##/**------------------------------------------------------------------------------------------**/
##/**          AMERICAN ELECTRIC POWER - EEMS Outage Data                              **/
##/**------------------------------------------------------------------------------------------**/
##/**                               Confidentiality Information:                               **/
##/**                               Copyright 2023 by                                          **/
##/**                               American Electric Power                                    **/
##/**                                                                                          **/
##/** This module is confidential and proprietary information of American Electric             **/
##/** Power, it is not to be copied or reproduced in any form, by any means, in                **/
##/** whole or in part, nor is it to be used for any purpose other than that for               **/
##/** which it is expressly provide without written permission of AEP.                         **/
##/**------------------------------------------------------------------------------------------**/
##/** AEP Custom Changes                                                                       **/
##/**  Version #   Name            Date        Description                                     **/
##/**   V0.1      Girish        05/11/2023  script for eems-outage-info-stg            **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/
## s3://aep-datalake-apps-dev/hdpapp/cties/glueetl/eems-outage-info-stg.py              **/
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

#VAR_OPCO = args['VAR_OPCO'] ## "pso" 
#VAR_OPCO = VAR_OPCO.lower()

#secret_name = "/aep/datalake/"+AWS_ENV+"/mv90/db/utl_login"
secret_name = args['VAR_SECRET_NAME'].lower()

S3_DATA_WORK = args['S3_DATA_WORK']
s3_data_work = S3_DATA_WORK.lower() + "-" + AWS_ENV
print('s3_data_work:',s3_data_work)

S3_DATA_CONSUME = args['S3_DATA_CONSUME']


final_path = s3_data_work+"/raw/dovs_eems/eems_outage_info_stg/"
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
sns_client = boto3.client("sns")


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
    secret_name = "/aep/datalake/"+AWS_ENV+"/mv90/db/utl_login"
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
try:
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
    ## Get meter premise data from ansersds through jdbc

    df_outage_info=sqlContext.read.format("jdbc") \
    .option("url",ConnectString).option("driver",DBDriver) \
    .option("fetchsize",100000) \
    .option("dbtable","(SELECT * FROM ANSERSDS.AEP_DL_OUTAGE_XFMR_VW)" ) \
    .option("user",VAR_USER).option("password",VAR_PASS) \
    .load()

    ##==============================================================================================##
    ## Transform the required cols accordingly 

    df_outage_info.printSchema()    

    ##==============================================================================================##
    ##Load the data in staging table

    df_outage_info.write.mode("overwrite") \
        .option("compression", "SNAPPY") \
        .option("spark.sql.files.maxRecordsPerFile", 1000000) \
        .format("parquet") \
        .save(final_path)


    print(f"DEBUG: Done with Writing to {final_path} Folder - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

    
    job.commit()
    sc.stop()
except Exception as e:
    print(e)
    msg_title=f"GlueJob: eems-outage-info-stg failed"
    print(msg_title)
    msg_body=f"## GlueJob: eems-outage-info-stg.py failed while processing"
    print(msg_body)
    #snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
    #raise e
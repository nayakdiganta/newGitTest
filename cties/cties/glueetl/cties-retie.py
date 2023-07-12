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
##/**   V0.1       Deepika                  01/24/2023      Retie Model in Spark       **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/

## Import Libraries -- Mostly Boilerplate
## These Libraries would change based on AWS Glue ETL Setup - Be Minimalistic
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Window
from pyspark.sql.functions import pandas_udf,udf
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import percentile_approx
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT

from pyspark.sql.types import IntegerType, StringType, DateType, StructType, StructField, DoubleType, FloatType
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit,unix_timestamp,posexplode,max
import json
import os
import sys
import boto3
import base64
import logging
import uuid
from datetime import datetime, date, timedelta
from time import time
t = time()

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import ClientError

import pyspark.sql.functions as F
from math import sqrt
import math
from scipy import spatial
import traceback
import pandas as pd
import numpy as np

##==============================================================================================##
## Collect Run Parameters 
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'AWS_ENV','S3_CTIES_CONSUME', 'VAR_OPCO','SSM_RUN_INTERVAL_NAME','SSM_LAST_RUN_AEP_USAGE_DT_TO_NAME','VAR_SNS_TOPIC'])

spark_appname = args['JOB_NAME']

AWS_ENV = args['AWS_ENV']
aws_env=AWS_ENV.lower()
# aws_env="dev"

VAR_OPCO = args['VAR_OPCO'] 
var_opco=VAR_OPCO.lower()
# var_opco="oh"

S3_CTIES_CONSUME = args['S3_CTIES_CONSUME']
#S3_CTIES_CONSUME = "s3://aep-datalake-consume"
s3_cties_consume = S3_CTIES_CONSUME.lower() + "-" + aws_env
print('DEBUG: s3_cties_consume',s3_cties_consume)

VAR_SNS_TOPIC = args['VAR_SNS_TOPIC'] ## ARN of SNS Topic
VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]
print('DEBUG: VAR_SNS_TOPIC_SUCCESS: ',VAR_SNS_TOPIC_SUCCESS)
print('DEBUG: VAR_SNS_TOPIC_FAILURE: ',VAR_SNS_TOPIC_FAILURE)


cties_RF_summary_basePath=s3_cties_consume + "/cties/meter_xfmr_voltg_rf/"
print('cties_RF_summary_basePath',cties_RF_summary_basePath)
cties_RF_summary_path=cties_RF_summary_basePath+"aep_opco="+var_opco

cties_mapping_basePath =s3_cties_consume + "/cties/meter_xfmr_mapping/"
##cties_mapping_basePath =s3_cties_consume + "/cties/meter_xfmr_mapping_bu/"
cties_mapping_path =cties_mapping_basePath +"aep_opco="+var_opco

cties_voltage_summary_basePath=s3_cties_consume + "/cties/meter_xfmr_voltg_summ/"   ## need to be modified
print('cties_voltage_summary_basePath',cties_voltage_summary_basePath)
cties_voltage_summary_path=cties_voltage_summary_basePath +"aep_opco="+var_opco

cties_voltg_summ_retie_basePath = s3_cties_consume + "/cties/meter_xfmr_voltg_summ_retie/"
cties_voltg_summ_retie_path=cties_voltg_summ_retie_basePath +"aep_opco="+var_opco

# get boto3 clients
sns_client=boto3.client('sns')
ssm_client = boto3.client('ssm')

##==============================================================================================##
## check RUN_TYPE value from ssm and define VAR_AEP_USAGE_DT_FROM/VAR_AEP_USAGE_DT_TO accordingly
## Get last_run_end_dt from ssm parameter which will be VAR_AEP_USAGE_DT_FROM for current run
VAR_AEP_USAGE_DT_FROM=''
VAR_AEP_USAGE_DT_TO=''

ssm_run_interval_name=args['SSM_RUN_INTERVAL_NAME']
ssm_run_interval_resp = ssm_client.get_parameter(Name=ssm_run_interval_name, WithDecryption=False)
VAR_RUN_INTERVAL_VAL = int(ssm_run_interval_resp['Parameter']['Value'])  ## default is 14

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
## Setup Spark Config 
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
job.commit()
conf=SparkConf()

current_user1 = sqlContext._sc.sparkUser()

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

# get sns_client
sns_client=boto3.client('sns')

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

##==============================================================================================##
##User Defined Functions
def dec(x):
    return int(str(x).split('.')[1][0])

def cartesian(latitude, longitude, elevation = 0):
    # Convert to radians
    latitude = latitude * (math.pi / 180)
    longitude = longitude * (math.pi / 180)

    R = 6371 # 6378137.0 + elevation  # relative to centre of the earth
    X = R * math.cos(latitude) * math.cos(longitude)
    Y = R * math.cos(latitude) * math.sin(longitude)
    Z = R * math.sin(latitude)
    return [X,Y,Z]
    
@pandas_udf(returnType=StructType([StructField('i',ArrayType(IntegerType())),StructField('di',ArrayType(DoubleType()))]))
def find_near_neigh_di(meter_cart_prod):
    np_cart = meter_cart_prod.apply(lambda x:np.array(x, dtype=np.float64))
    d,i = tree.query(np_cart.tolist(),k = 10)
    i_list = list(i)
    c_list = list(d)
    c_list2 = [i *3280.84 for i in c_list]
    out = pd.DataFrame({"i":i_list,"di":c_list2})
    return out
 
norm_udf = F.udf(lambda x: DenseVector(x).norm(2).item(), FloatType())

def normalized_array(x, norm):
    return Vectors.dense(x) / norm

def euclidean_distance(x, y):
    return sqrt(Vectors.squared_distance(x, y))     
##==================================================================================================##
##Main Loop
# try:
        
rf_schema=StructType([\
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
        StructField('median_eucl_dist',FloatType(),True),\
        StructField('median_shape_mismatch_cnt',IntegerType(),True),\
        StructField('median_z_score_eucl_dist',DoubleType(),True),\
        StructField('y_pred',IntegerType(),True),\
        StructField('pred_0',DoubleType(),True),\
        StructField('pred_1',DoubleType(),True),\
        StructField('run_control_id',LongType(),True),\
        StructField('aws_update_dttm',StringType(),True),\
        StructField('aep_opco',StringType(),True),\
        StructField('aep_run_dt',DateType(),True)\
        ])

rf_src_read=spark.read.schema(rf_schema) \
        .option("basePath", cties_RF_summary_basePath) \
        .option("inferSchema", "false") \
        .option("compression", "snappy") \
        .option("mergeSchema", "false") \
        .format("parquet") \
        .load(path=cties_RF_summary_path)
        
rf_src_read.printSchema()

rf_src_date = rf_src_read.select(max(col('aep_run_dt'))).head()[0]
rf_data = rf_src_read.filter(col('aep_run_dt') == rf_src_date)    
print("DEBUG: Consume table RF data= "  + str(rf_data.count()))

    
#dec_udf1 = F.udf(lambda x: str(x).split('.')[1][0], IntegerType())
dec_udf = F.udf(lambda x: str(x).split('.')[1][0], StringType())
rf_decile = rf_data \
            .withColumn("decile", dec_udf('pred_1'))
    
# Earlier it is 5 now we modified as 7
rf4_df = rf_decile.filter(rf_decile.decile >= 7) 
    
rf4_df.printSchema()
print("DEBUG: decile - count = "  + str(rf4_df.count()))

#Get the unique transformer and its products

map_schema=StructType([\
        StructField('aep_premise_nb',StringType(),True),\
        StructField('serialnumber',StringType(),True),\
        StructField('aep_premise_loc_nb',StringType(),True),\
        StructField('aep_premise_long',StringType(),True),\
        StructField('aep_premise_lat',StringType(),True),\
        StructField('trsf_pole_nb',StringType(),True),\
        StructField('xfmr_long',StringType(),True),\
        StructField('xfmr_lat',StringType(),True),\
        StructField('trsf_mount_cd',StringType(),True),\
        StructField('gis_circuit_nb',StringType(),True),\
        StructField('gis_circuit_nm',StringType(),True),\
        StructField('gis_station_nb',StringType(),True),\
        StructField('gis_station_nm',StringType(),True),\
        StructField('company_cd',StringType(),True),\
        StructField('srvc_entn_cd',StringType(),True),\
        StructField('distance_feets',DoubleType(),True),\
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
        StructField('aws_update_dttm',StringType(),True),\
        StructField('run_control_id',LongType(),True),\
        StructField('aep_opco',StringType(),True),\
        StructField('asof_dt',DateType(),True)\
        ])

map_read=spark.read.schema(map_schema) \
        .option("basePath", cties_mapping_basePath) \
        .option("inferSchema", "false") \
        .option("compression", "snappy") \
        .option("mergeSchema", "false") \
        .format("parquet") \
        .load(path=cties_mapping_path)
        
map_read.printSchema()
print("DEBUG: Mapping Consume table data= "  + str(map_read.count()))

max_date_map = map_read.select(max(col('asof_dt'))).head()[0]
map_data = map_read.filter(col('asof_dt') == max_date_map)    


trans_src = map_data.select('trsf_pole_nb','xfmr_long','xfmr_lat').distinct()
trans = trans_src \
        .withColumn('lat_t',col('xfmr_lat').cast('double')) \
        .withColumn('long_t',col('xfmr_long').cast('double')) \
        .withColumn("cart_prod", F.udf(cartesian, ArrayType(DoubleType()))("lat_t", "long_t")) \
        .select('trsf_pole_nb','xfmr_long','xfmr_lat','cart_prod')

print("DEBUG: Unique Transformer - count = "  + str(trans.count()))
#trans.printSchema()


# Get the Meter data
meters2 = map_data.alias('mploc').join(rf4_df.alias('rfdf'),['serialnumber'],how="inner") \
            .select('mploc.aep_premise_nb','mploc.serialnumber','mploc.aep_premise_loc_nb','mploc.aep_premise_long','mploc.aep_premise_lat','mploc.trsf_pole_nb','mploc.xfmr_long','mploc.xfmr_lat')

meters3 = meters2 \
            .withColumnRenamed("trsf_pole_nb","curr_trsf") \
            .withColumn('lat_m',col('aep_premise_lat').cast('double')) \
            .withColumn('long_m',col('aep_premise_long').cast('double')) \
            .withColumn("meter_cart_prod", F.udf(cartesian, ArrayType(DoubleType()))("lat_m", "long_m"))

#meters3.show(2)
print("DEBUG: Unique Meter - count = "  + str(meters3.count()))
#meters3.printSchema()

trans_p = trans.toPandas()
trans_pr = trans_p.reset_index()


transformer = [i.tolist() for i in trans_pr['cart_prod']]
print("DEBUG: Transformercoord",transformer[:3])
tree = spatial.KDTree(transformer)

new = meters3.withColumn('prod',find_near_neigh_di(meters3['meter_cart_prod']))
new.printSchema()
print("DEBUG: The function output - count = "  + str(new.count()))

trsf_explode = new.select('serialnumber',posexplode('prod.i').alias('pos','indices'))
dist_explode = new.select('serialnumber',posexplode('prod.di').alias('pos','dist'))
new2 = trsf_explode.join(dist_explode,['serialnumber','pos'])

trans_spk = spark.createDataFrame(trans_pr)
new2.printSchema()
trans_spk.printSchema()
new3 = new2.alias('n').join(trans_spk.alias('t'),col('t.index') == col('n.indices'),how="inner" )


new4 = new3.join(new,['serialnumber'],'inner') \
            .select('serialnumber','aep_premise_nb','aep_premise_loc_nb','curr_trsf','trsf_pole_nb','dist')
new4.printSchema()
    

windowSpec = Window.partitionBy("serialnumber").orderBy("dist")
new5 = new4.withColumn("tran_rank",dense_rank().over(windowSpec))
new5.printSchema()

#### New5 is dataframe that contains the meters in question and their corresponding 10 closest transformers
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
    
#cns_mtr_vltg_mp.printSchema()
print("DEBUG: Consume table data= "  + str(cns_mtr_vltg_mp.count()))

new_data =cns_mtr_vltg_mp.select("trsf_pole_nb","xf_read_array_whole","aep_usage_dt").distinct()
new_meter=cns_mtr_vltg_mp.select('serialnumber','trsf_pole_nb','read_array','aep_usage_dt')

nd3 = new_data.join(new5,['trsf_pole_nb'])
nd4 = nd3.alias('n').join(new_meter.alias('m'),['serialnumber','aep_usage_dt']).select('n.serialnumber','n.aep_usage_dt','n.trsf_pole_nb','n.xf_read_array_whole', 'n.dist','n.curr_trsf','n.tran_rank','m.read_array')



##==============================================================================================##
## loop through days and find euclidean distance for each meter and transformer combo

all_d = nd4 \
            .withColumn("array_norm", norm_udf("read_array")) \
            .withColumn("xf_array_norm", norm_udf("xf_read_array_whole")) \
            .withColumn("array_normalized", F.udf(normalized_array, VectorUDT())("read_array", "array_norm")) \
            .withColumn("xf_normalized", F.udf(normalized_array, VectorUDT())("xf_read_array_whole", "xf_array_norm")) \
            .withColumn("ed_dist", F.udf(euclidean_distance, FloatType())("array_normalized", "xf_normalized")) \
            .select('serialnumber','curr_trsf','read_array', 'trsf_pole_nb', 'xf_read_array_whole', 'dist', 'tran_rank','ed_dist','aep_usage_dt')


### Finding the transformer with the smallest ED score as the best fit
#print("debug: Getting the entire columns")
#all_d.printSchema()
print("DEBUG: The Calculations output - row count = "  + str(all_d.count())) 


##==============================================================================================##
## Find the median of the transformer to the meter

final2_stats =all_d \
                .groupby(['serialnumber', 'trsf_pole_nb'])
final2_stats = final2_stats.agg(percentile_approx('ed_dist', 0.5).alias('median_ed_dist'))

semi_df = all_d.alias("ad") \
    .join(final2_stats.alias("s") , ['serialnumber', 'trsf_pole_nb'], how="left_outer") \
    .select('serialnumber','curr_trsf', 'trsf_pole_nb', 'dist', 'tran_rank','ed_dist','median_ed_dist').distinct()

## Find the min ecul_dist for every meters
final2_min = semi_df.groupby('serialnumber')
final2_min = final2_min.agg({"ed_dist": "min"}) \
                        .withColumnRenamed('min(ed_dist)','ed_dist')

score_df = semi_df.alias('sd') \
            .join(final2_min.alias('m'),['serialnumber','ed_dist'],how = 'inner')
    
print("DEBUG: The function final2_stats output - count = "  + str(semi_df.count())) 



bef_final_df = score_df.alias('s').join(rf4_df.alias('r'),('serialnumber'),how='inner') \
                .select('s.*','r.aep_premise_nb','r.median_eucl_dist','r.median_z_score_eucl_dist','r.y_pred','r.pred_0','r.pred_1','r.distance_feets','r.decile','r.gis_circuit_nb','r.gis_circuit_nm','r.gis_station_nb','r.gis_station_nm','r.xf_meter_cnt','r.median_shape_mismatch_cnt')
bef_final_df = bef_final_df \
                    .withColumnRenamed('trsf_pole_nb', 'new_trsf_pole_nb') \
                    .withColumnRenamed('curr_trsf', 'curr_trsf_pole_nb') \
                    .withColumnRenamed('median_eucl_dist','orig_median_eucl_dist') \
                    .withColumnRenamed('median_z_score_eucl_dist','orig_median_z_score_eucl_dist') \
                    .withColumnRenamed('ed_dist','pred_eucl_dist') \
                    .withColumnRenamed('median_ed_dist','pred_median_eucl_dist') \
                    .select('aep_premise_nb','serialnumber','trsf_mount_cd','gis_circuit_nb','gis_circuit_nm','gis_station_nb','gis_station_nm','curr_trsf_pole_nb','distance_feets','xf_meter_cnt','median_shape_mismatch_cnt','orig_median_eucl_dist','orig_median_z_score_eucl_dist','y_pred','pred_0','pred_1','decile','new_trsf_pole_nb','pred_eucl_dist','dist','tran_rank','pred_median_eucl_dist')
bef_final_df.printSchema()

##==============================================================================================##
##  Writing into the Retie Table with the desired columns

##final_df = bef_final_df \
##    .withColumn('aep_run_dt', lit(datetime.today().strftime("%Y-%m-%d"))) \
##    .withColumn('aep_opco', lit(var_opco))

## Writing to the Final Table
cns_mtr_vltg_mp = cns_mtr_vltg_mp \
                    .select("aep_premise_nb","serialnumber","trsf_pole_nb","trsf_mount_cd","gis_circuit_nb", 
                "gis_circuit_nm","gis_station_nb","gis_station_nm","company_cd","srvc_entn_cd","distance_feets","xf_meter_cnt","gis_mapping_asof_dt",
                "median_eucl_dist","median_shape_mismatch_cnt","median_z_score_eucl_dist","run_control_id","aep_opco").distinct()

final_vltg = map_data.alias('mr').join(cns_mtr_vltg_mp.alias('mp'),'serialnumber',how='inner') \
                                .select('mr.serialnumber','mr.aep_premise_nb','mr.aep_premise_loc_nb','mr.aep_premise_long','mr.aep_premise_lat','mr.trsf_pole_nb','mr.xfmr_long','mr.xfmr_lat','mr.trsf_mount_cd','mr.gis_circuit_nb','mr.gis_circuit_nm','mr.gis_station_nb','mr.gis_station_nm','mr.company_cd','mr.srvc_entn_cd','mr.distance_feets','mr.county_cd','mr.county_nm','mr.district_nb','mr.district_nm','mr.type_srvc_cd','mr.type_srvc_cd_desc','mr.srvc_addr_1_nm','mr.srvc_addr_2_nm','mr.srvc_addr_3_nm','mr.srvc_addr_4_nm','mr.serv_city_ad','mr.state_cd','mr.serv_zip_ad','mp.run_control_id','mp.gis_mapping_asof_dt','mp.median_eucl_dist','mp.median_shape_mismatch_cnt','mp.median_z_score_eucl_dist','mp.xf_meter_cnt')
                                
print("Mapping data count" + str(map_read.count()))
print("cns_mtr_vltg_mp data count" + str(cns_mtr_vltg_mp.count()))
print("final_vltg data count" + str(final_vltg.count()))
print("bef_final_df data count" + str(bef_final_df.count()))


final_rt = final_vltg.alias('f').join(bef_final_df.alias('b'),'serialnumber',how='inner') \
                    .select('f.*','b.curr_trsf_pole_nb','b.orig_median_eucl_dist','b.orig_median_z_score_eucl_dist','b.y_pred','b.pred_0','b.pred_1','b.decile','b.new_trsf_pole_nb','b.pred_eucl_dist','b.dist','b.tran_rank','b.pred_median_eucl_dist').distinct()
print("final_rt data count" + str(final_rt.count()))


final_rt.printSchema()
final_rt = final_rt \
    .withColumn('aep_opco',lit(VAR_OPCO)) \
    .withColumn('aep_run_dt', lit(datetime.today().strftime("%Y-%m-%d")))
    

final_rt.write.mode("overwrite") \
    .partitionBy("aep_opco", "aep_run_dt") \
    .format("parquet") \
    .option("compression", "SNAPPY") \
    .option("spark.sql.files.maxRecordsPerFile", 250000) \
    .save(cties_voltg_summ_retie_basePath)

print("DEBUG: Done with Writing to Retie Mapping Folder - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

print("DEBUG: Starting MSCK Repair - "  + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
spark.sql("msck repair table cties.meter_xfmr_voltg_summ_retie")     
##==============================================================================================##    
##update the ssm parameter value for next run
print("DEBUG: updating ssm parameter values for next run..")
ssm_last_run_aep_usage_dt_to_resp = ssm_client.put_parameter(Name=ssm_last_run_aep_usage_dt_to_name, Value= VAR_AEP_USAGE_DT_TO , Type='String', Overwrite=True )
# ssm_run_type_resp = ssm_client.put_parameter(Name=ssm_run_type_name, Value='s' , Type='String', Overwrite=True )

job.commit()
sc.stop() 
# except Exception as e:
#     print(e)
#     msg_title=f"GlueJob: cties-retie failed"
#     print(msg_title)
#     msg_body=f"## GlueJob: cties-retie.py failed while processing for OPCO: <{VAR_OPCO}> for date range {VAR_AEP_USAGE_DT_FROM} and {VAR_AEP_USAGE_DT_TO}"
#     print(msg_body)
#     snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
#     raise e     
    
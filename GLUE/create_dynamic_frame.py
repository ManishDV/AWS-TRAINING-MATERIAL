
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)
schema = ['id','name']
data = [(1,'ABC'),(2,'DEF'),(3,'GHI'),(4,'JKL')]

rdd = sc.parallelize(data)
spark_df = rdd.toDF(schema)

#creating dynamicFrame
#Using DynamicFrame Class, this is used to create dynamicframe from spark dataframe
#first method
dyf = DynamicFrame(spark_df,glueContext,'dyf')
print("First Method Count: ",dyf.count())

#Second Method
dyf = DynamicFrame.fromDF(spark_df,glueContext,'dyf')
print("Second Method Count: ",dyf.count())

#Using glueContext Methods
#these are used to create DynamicFrame from data sources

dyf = glueContext.create_dynamic_frame_from_rdd(data,'dyf',schema=schema)
print("create_dynamic_frame_from_rdd count: ", dyf.count())

dyf = glueContext.create_dynamic_frame_from_catalog('legislation','persons_json')
print("create_dynamic_frame_from_catalog count: ", dyf.count())

#Creating dataframe directly from s3 (without catalog)
dyf = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':['s3://awsglue-datasets/examples/us-legislators/all/persons.json']},format='json')
print("create_dynamic_frame_from_options count: ", dyf.count())






# Add parameters while starting job using boto3 glue client using Arguments parameter or 
#from console go to security configuration and add arguments in job parameters sections (include hypens)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys
from awsglue.utils import getResolvedOptions


glueContext = GlueContext(SparkContext.getOrCreate())

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'day_partition_key',
                           'hour_partition_key'])
                           
print( "The day-partition key is: ", args['day_partition_key'])
print( "The hour-partition key is: ", args['hour_partition_key'])
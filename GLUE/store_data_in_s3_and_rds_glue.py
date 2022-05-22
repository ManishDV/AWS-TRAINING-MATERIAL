# public s3 bucket for input dataset `s3://awsglue-datasets/examples/us-legislators/all`
#Steps to generate table metadata in glue catalog
#1. Create crawler with appropriate IAM role and above s3 bucket for data store
#2. create connection in catalog and configure it to use rds endpoint and other credentials
#3. use name of this connection in from_jdbc_conf() call, as a value of catalog_connection parameter
#4. While writing to s3 use appropriate connection_options


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())




persons = glueContext.create_dynamic_frame.from_catalog(
             database="legislation",
             table_name="persons_json")
print ("Persons Count: ", persons.count())



memberships = glueContext.create_dynamic_frame.from_catalog(
                 database="legislation",
                 table_name="memberships_json")
print( "Membership Count: ", memberships.count())


orgs = glueContext.create_dynamic_frame.from_catalog(
          database="legislation",
          table_name="organizations_json")
print ("Org Count: ", orgs.count())



orgs = orgs.drop_fields(['other_names',
                        'identifiers']).rename_field(
                            'id', 'org_id').rename_field(
                              'name', 'org_name')


l_history = Join.apply(orgs,
                      Join.apply(persons, memberships, 'id', 'person_id'),
                      'org_id', 'organization_id').drop_fields(['person_id', 'org_id'])
print ("L_HOSTORY Count: ", l_history.count())



glueContext.write_dynamic_frame.from_options(frame = l_history,
          connection_type = "s3",
          connection_options = {"path": "s3://manishpractice/output-dir/legislator_history"},
          format = "parquet")



dfc = l_history.relationalize("hist_root", "s3://tempdiravi/temp-dir/")
print(dfc.keys())

for df_name in dfc.keys():
  m_df = dfc.select(df_name)
  print ("Writing to table: ", df_name)
  glueContext.write_dynamic_frame.from_jdbc_conf(frame = m_df, catalog_connection='rds_con',connection_options = {"dbtable": df_name, "database": "legislation"})


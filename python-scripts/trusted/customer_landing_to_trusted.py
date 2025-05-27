import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1748306725127 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1748306725127")

# Script generated for node SQL Query
SqlQuery817 = '''
SELECT *
FROM `stedi-db-bj`.`customer_landing`
WHERE sharewithresearchasofdate IS NOT NULL 
'''
SQLQuery_node1748307100276 = sparkSqlQuery(glueContext, query = SqlQuery817, mapping = {"myDataSource":AWSGlueDataCatalog_node1748306725127}, transformation_ctx = "SQLQuery_node1748307100276")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1748307100276, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748306701253", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748307266735 = glueContext.getSink(path="s3://wgu-stedi-bjordan-data/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1748307266735")
AmazonS3_node1748307266735.setCatalogInfo(catalogDatabase="stedi-db-bj",catalogTableName="customer_trusted")
AmazonS3_node1748307266735.setFormat("json")
AmazonS3_node1748307266735.writeFrame(SQLQuery_node1748307100276)
job.commit()
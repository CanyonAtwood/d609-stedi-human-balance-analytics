import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

CustomerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node2",
)

# Transform - SQL Query
# Inner join accelerometer readings to consented customers on email.
# The accelerometer "user" column holds the customer email address.
# SELECT a.* keeps only the accelerometer columns, as the rubric requires.
SqlQuery0 = """
SELECT a.*
FROM accelerometer_landing AS a
INNER JOIN customer_trusted AS c
        ON a.user = c.email
"""

AccelerometerPrivacyJoin_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1,
        "customer_trusted": CustomerTrusted_node2,
    },
    transformation_ctx="AccelerometerPrivacyJoin_node3",
)

AccelerometerTrusted_node4 = glueContext.getSink(
    path="s3://stedi-canyon-2026/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node4",
)
AccelerometerTrusted_node4.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node4.setFormat("json")
AccelerometerTrusted_node4.writeFrame(AccelerometerPrivacyJoin_node3)

job.commit()

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


CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# Transform - SQL Query
# Keep consented customers who ALSO produced accelerometer readings.
# DISTINCT collapses the many-readings-per-customer fan-out back to one row
# per customer, which is what keeps the count at 482 instead of 40,981.
SqlQuery0 = """
SELECT DISTINCT c.*
FROM customer_trusted AS c
INNER JOIN accelerometer_trusted AS a
        ON c.email = a.user
"""

CustomerCuratedJoin_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": CustomerTrusted_node1,
        "accelerometer_trusted": AccelerometerTrusted_node2,
    },
    transformation_ctx="CustomerCuratedJoin_node3",
)

CustomerCurated_node4 = glueContext.getSink(
    path="s3://<YOUR-BUCKET>/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node4",
)
CustomerCurated_node4.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node4.setFormat("json")
CustomerCurated_node4.writeFrame(CustomerCuratedJoin_node3)

job.commit()

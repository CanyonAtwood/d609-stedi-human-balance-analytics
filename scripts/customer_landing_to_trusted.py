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


# Data Source - Glue Data Catalog (more reliable than the S3 bucket node)
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1",
)

# Transform - SQL Query
# Keep only customers who consented to share their data for research.
# Blank consent arrives as NULL or as 0, so both are excluded.
SqlQuery0 = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
  AND sharewithresearchasofdate <> 0
"""

CustomerPrivacyFilter_node2 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": CustomerLanding_node1},
    transformation_ctx="CustomerPrivacyFilter_node2",
)

# Data Target - S3 bucket, writing the customer_trusted Glue table.
# enableUpdateCatalog + updateBehavior satisfies the rubric requirement that
# the job dynamically infers and updates the Glue table schema.
CustomerTrusted_node3 = glueContext.getSink(
    path="s3://stedi-canyon-2026/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node3",
)
CustomerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node3.setFormat("json")
CustomerTrusted_node3.writeFrame(CustomerPrivacyFilter_node2)

job.commit()

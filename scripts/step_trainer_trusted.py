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


StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1",
)

CustomerCurated_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node2",
)

# Transform - SQL Query
# Keep Step Trainer readings whose serial number belongs to a curated customer.
#
# Background: the fulfillment website had a defect and reused the same 30 serial
# numbers across millions of customer records, so the customer serial number is
# not a unique identifier. Joining through customer_curated first restricts the
# match to the small set of consented customers who actually have a device,
# which is what makes this join usable.
#
# SELECT s.* keeps only Step Trainer columns. DISTINCT guards against the
# duplicate serial numbers in customer_curated fanning the reading count out.
SqlQuery0 = """
SELECT DISTINCT s.*
FROM step_trainer_landing AS s
INNER JOIN customer_curated AS c
        ON s.serialnumber = c.serialnumber
"""

StepTrainerJoin_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1,
        "customer_curated": CustomerCurated_node2,
    },
    transformation_ctx="StepTrainerJoin_node3",
)

StepTrainerTrusted_node4 = glueContext.getSink(
    path="s3://<YOUR-BUCKET>/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node4",
)
StepTrainerTrusted_node4.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node4.setFormat("json")
StepTrainerTrusted_node4.writeFrame(StepTrainerJoin_node3)

job.commit()

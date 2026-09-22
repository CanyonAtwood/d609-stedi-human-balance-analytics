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


StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# Transform - SQL Query
# Pair each Step Trainer reading with the accelerometer reading taken at the
# same instant. Both sides store epoch milliseconds, so the match is exact
# equality between sensorreadingtime and timestamp.
# This is the training set the data science team will use.
SqlQuery0 = """
SELECT s.sensorreadingtime,
       s.serialnumber,
       s.distancefromobject,
       a.user,
       a.x,
       a.y,
       a.z
FROM step_trainer_trusted AS s
INNER JOIN accelerometer_trusted AS a
        ON s.sensorreadingtime = a.timestamp
"""

MachineLearningJoin_node3 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1,
        "accelerometer_trusted": AccelerometerTrusted_node2,
    },
    transformation_ctx="MachineLearningJoin_node3",
)

MachineLearningCurated_node4 = glueContext.getSink(
    path="s3://<YOUR-BUCKET>/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node4",
)
MachineLearningCurated_node4.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node4.setFormat("json")
MachineLearningCurated_node4.writeFrame(MachineLearningJoin_node3)

job.commit()

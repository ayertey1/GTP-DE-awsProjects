import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node RelationalDB.apartment_attributes
RelationalDBapartment_attributes_node1750117477408 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartment_attributes",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDBapartment_attributes_node1750117477408"
)

# Script generated for node RelationalDB.apartments
RelationalDBapartments_node1750117369324 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartments",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDBapartments_node1750117369324"
)

# Script generated for node RelationalDB.bookings
RelationalDBbookings_node1750117160308 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "bookings",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDBbookings_node1750117160308"
)

# Script generated for node RelationalDB.user_viewing
RelationalDBuser_viewing_node1750117601875 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "user_viewing",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDBuser_viewing_node1750117601875"
)

# Script generated for node AmazonS3.apartment_attributes
EvaluateDataQuality().process_rows(frame=RelationalDBapartment_attributes_node1750117477408, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750113377201", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDBapartment_attributes_node1750117477408.count() >= 1):
   RelationalDBapartment_attributes_node1750117477408 = RelationalDBapartment_attributes_node1750117477408.coalesce(1)
AmazonS3apartment_attributes_node1750117536837 = glueContext.write_dynamic_frame.from_options(frame=RelationalDBapartment_attributes_node1750117477408, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-marketplace-etl-job/raw/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3apartment_attributes_node1750117536837")

# Script generated for node AmazonS3.apartments
EvaluateDataQuality().process_rows(frame=RelationalDBapartments_node1750117369324, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750113377201", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDBapartments_node1750117369324.count() >= 1):
   RelationalDBapartments_node1750117369324 = RelationalDBapartments_node1750117369324.coalesce(1)
AmazonS3apartments_node1750117416148 = glueContext.write_dynamic_frame.from_options(frame=RelationalDBapartments_node1750117369324, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-marketplace-etl-job/raw/apartments/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3apartments_node1750117416148")

# Script generated for node AmazonS3.bookings
EvaluateDataQuality().process_rows(frame=RelationalDBbookings_node1750117160308, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750113377201", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDBbookings_node1750117160308.count() >= 1):
   RelationalDBbookings_node1750117160308 = RelationalDBbookings_node1750117160308.coalesce(1)
AmazonS3bookings_node1750117295556 = glueContext.write_dynamic_frame.from_options(frame=RelationalDBbookings_node1750117160308, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-marketplace-etl-job/raw/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3bookings_node1750117295556")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDBuser_viewing_node1750117601875, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750113377201", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDBuser_viewing_node1750117601875.count() >= 1):
   RelationalDBuser_viewing_node1750117601875 = RelationalDBuser_viewing_node1750117601875.coalesce(1)
AmazonS3_node1750117656850 = glueContext.write_dynamic_frame.from_options(frame=RelationalDBuser_viewing_node1750117601875, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-marketplace-etl-job/raw/user_viewing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750117656850")

job.commit()
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AmazonS3.booking
AmazonS3booking_node1750117876364 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-marketplace-etl-job/raw/bookings/"], "recurse": True}, transformation_ctx="AmazonS3booking_node1750117876364")

# Script generated for node AmazonS3.user_viewing
AmazonS3user_viewing_node1750118633141 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-marketplace-etl-job/raw/user_viewing/"], "recurse": True}, transformation_ctx="AmazonS3user_viewing_node1750118633141")

# Script generated for node AmazonS3.apartment_attributes
AmazonS3apartment_attributes_node1750118487148 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-marketplace-etl-job/raw/apartment_attributes/"], "recurse": True}, transformation_ctx="AmazonS3apartment_attributes_node1750118487148")

# Script generated for node AmazonS3.apartments
AmazonS3apartments_node1750117971686 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-marketplace-etl-job/raw/apartments/"], "recurse": True}, transformation_ctx="AmazonS3apartments_node1750117971686")

# Script generated for node AmazonRedshift.bookings
AmazonRedshiftbookings_node1750117938805 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3booking_node1750117876364, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date VARCHAR, checkin_date VARCHAR, checkout_date VARCHAR, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="AmazonRedshiftbookings_node1750117938805")

# Script generated for node AmazonRedshift.user_viewing
AmazonRedshiftuser_viewing_node1750118771206 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3user_viewing_node1750118633141, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.user_viewing", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.user_viewing (user_id INTEGER, apartment_id INTEGER, viewed_at VARCHAR, is_wishlisted VARCHAR, call_to_action VARCHAR);"}, transformation_ctx="AmazonRedshiftuser_viewing_node1750118771206")

# Script generated for node AmazonRedshift.apartment_attributes
AmazonRedshiftapartment_attributes_node1750118580835 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3apartment_attributes_node1750118487148, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo VARCHAR, pets_allowed VARCHAR, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL);"}, transformation_ctx="AmazonRedshiftapartment_attributes_node1750118580835")

# Script generated for node AmazonRedshift.apartments
AmazonRedshiftapartments_node1750118245352 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3apartments_node1750117971686, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raws.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on VARCHAR, is_active VARCHAR, last_modified_timestamp VARCHAR);"}, transformation_ctx="AmazonRedshiftapartments_node1750118245352")

job.commit()
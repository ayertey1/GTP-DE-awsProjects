import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node apartments.Transform
def TransformApartments(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, to_date

    # Get the first (and usually only) DynamicFrame from the input collection
    dyf = dfc.select(list(dfc.keys())[0])

    # Convert to DataFrame
    df = dyf.toDF()

    # Apply transformations
    df_transformed = df \
        .withColumn("is_active", when(col("is_active") == "TRUE", True).otherwise(False)) \
        .withColumn("listing_created_on", to_date(col("listing_created_on"), "dd/MM/yyyy")) \
        .withColumn("last_modified_timestamp", to_date(col("last_modified_timestamp"), "dd/MM/yyyy")) 

    # Convert back to DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")

    # Return as DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)
# Script generated for node user_viewing.Transform
def TransformUserViewing(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, to_date

    # Get the first (and usually only) DynamicFrame from the input collection
    dyf = dfc.select(list(dfc.keys())[0])

    # Convert to DataFrame
    df = dyf.toDF()

    # Apply transformations
    df_transformed = df \
        .withColumn("is_wishlisted", when(col("is_wishlisted") == "TRUE", True).otherwise(False)) \
        .withColumn("viewed_at", to_date(col("viewed_at"), "dd/MM/yyyy")) 

    # Convert back to DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")

    # Return as DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)
# Script generated for node Bookings.Transform
def TransformBookings(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, to_date

    # Get the first (and usually only) DynamicFrame from the input collection
    dyf = dfc.select(list(dfc.keys())[0])

    # Convert to DataFrame
    df = dyf.toDF()

    # Apply transformations
    df_transformed = df \
        .withColumn("booking_date", to_date(col("booking_date"), "dd/MM/yyyy")) \
        .withColumn("checkin_date", to_date(col("checkin_date"), "dd/MM/yyyy")) \
        .withColumn("checkout_date", to_date(col("checkout_date"), "dd/MM/yyyy"))

    # Convert back to DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")

    # Return as DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)
# Script generated for node apartment_attributesTransform
def TransformApartmentAttributes(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, to_date

    # Get the first (and usually only) DynamicFrame from the input collection
    dyf = dfc.select(list(dfc.keys())[0])

    # Convert to DataFrame
    df = dyf.toDF()

    # Apply transformations
    df_transformed = df \
        .withColumn("has_photo", when(col("has_photo") == "TRUE", True).otherwise(False)) \
        .withColumn("pets_allowed", when(col("pets_allowed") == "TRUE", True).otherwise(False)) 

    # Convert back to DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")

    # Return as DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)
     
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node rawRedshift.user_viewing
rawRedshiftuser_viewing_node1750159394203 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.user_viewing", "connectionName": "Redshift connection"}, transformation_ctx="rawRedshiftuser_viewing_node1750159394203")

# Script generated for node rawRedshift.apartment_attributes
rawRedshiftapartment_attributes_node1750157495487 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartment_attributes", "connectionName": "Redshift connection"}, transformation_ctx="rawRedshiftapartment_attributes_node1750157495487")

# Script generated for node rawRedshift.apartments
rawRedshiftapartments_node1750158102290 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.apartments", "connectionName": "Redshift connection"}, transformation_ctx="rawRedshiftapartments_node1750158102290")

# Script generated for node rawRedshift.bookings
rawRedshiftbookings_node1750156367146 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raws.bookings", "connectionName": "Redshift connection"}, transformation_ctx="rawRedshiftbookings_node1750156367146")

# Script generated for node user_viewing.Transform
user_viewingTransform_node1750159502090 = TransformUserViewing(glueContext, DynamicFrameCollection({"rawRedshiftuser_viewing_node1750159394203": rawRedshiftuser_viewing_node1750159394203}, glueContext))

# Script generated for node apartment_attributesTransform
apartment_attributesTransform_node1750157571458 = TransformApartmentAttributes(glueContext, DynamicFrameCollection({"rawRedshiftapartment_attributes_node1750157495487": rawRedshiftapartment_attributes_node1750157495487}, glueContext))

# Script generated for node apartments.Transform
apartmentsTransform_node1750158483831 = TransformApartments(glueContext, DynamicFrameCollection({"rawRedshiftapartments_node1750158102290": rawRedshiftapartments_node1750158102290}, glueContext))

# Script generated for node Bookings.Transform
BookingsTransform_node1750156440509 = TransformBookings(glueContext, DynamicFrameCollection({"rawRedshiftbookings_node1750156367146": rawRedshiftbookings_node1750156367146}, glueContext))

# Script generated for node Collection.user_viewing
Collectionuser_viewing_node1750159705409 = SelectFromCollection.apply(dfc=user_viewingTransform_node1750159502090, key=list(user_viewingTransform_node1750159502090.keys())[0], transformation_ctx="Collectionuser_viewing_node1750159705409")

# Script generated for node Collections.apartment_attributes
Collectionsapartment_attributes_node1750157869634 = SelectFromCollection.apply(dfc=apartment_attributesTransform_node1750157571458, key=list(apartment_attributesTransform_node1750157571458.keys())[0], transformation_ctx="Collectionsapartment_attributes_node1750157869634")

# Script generated for node Collection.apartments
Collectionapartments_node1750158990070 = SelectFromCollection.apply(dfc=apartmentsTransform_node1750158483831, key=list(apartmentsTransform_node1750158483831.keys())[0], transformation_ctx="Collectionapartments_node1750158990070")

# Script generated for node Collection.bookings
Collectionbookings_node1750156833627 = SelectFromCollection.apply(dfc=BookingsTransform_node1750156440509, key=list(BookingsTransform_node1750156440509.keys())[0], transformation_ctx="Collectionbookings_node1750156833627")

# Script generated for node curated_Redshift.user_viewing
curated_Redshiftuser_viewing_node1750159789471 = glueContext.write_dynamic_frame.from_options(frame=Collectionuser_viewing_node1750159705409, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.user_viewing", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.user_viewing (user_id INTEGER, apartment_id INTEGER, viewed_at DATE, is_wishlisted BOOLEAN, call_to_action VARCHAR);"}, transformation_ctx="curated_Redshiftuser_viewing_node1750159789471")

# Script generated for node curated_Redshift.apartment_attributes
curated_Redshiftapartment_attributes_node1750157918874 = glueContext.write_dynamic_frame.from_options(frame=Collectionsapartment_attributes_node1750157869634, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL);"}, transformation_ctx="curated_Redshiftapartment_attributes_node1750157918874")

# Script generated for node curated_Redshift.apartments
curated_Redshiftapartments_node1750159084743 = glueContext.write_dynamic_frame.from_options(frame=Collectionapartments_node1750158990070, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp DATE);"}, transformation_ctx="curated_Redshiftapartments_node1750159084743")

# Script generated for node curated_Redshift.Bookings
curated_RedshiftBookings_node1750156873004 = glueContext.write_dynamic_frame.from_options(frame=Collectionbookings_node1750156833627, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-102306345968-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date DATE, checkin_date DATE, checkout_date DATE, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="curated_RedshiftBookings_node1750156873004")

job.commit()
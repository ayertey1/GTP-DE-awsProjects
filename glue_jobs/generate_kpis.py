import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, date_trunc, avg, sum as _sum, count, when, datediff, current_date

# Get job args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Spark config optimization
spark.conf.set("spark.sql.shuffle.partitions", "100")  # reduce small shuffles

# Connection details
redshift_url = "jdbc:redshift://redshift-rental-marketplace.ckibeoxzu28g.eu-north-1.redshift.amazonaws.com:5439/dev"
temp_dir = "s3://rental-marketplace-etl-lab/temp/"
user = "admin"
password = "Phableout*00"

# Helper to load Redshift table with optimization
def load_redshift_table(query_alias: str):
    return spark.read \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", query_alias) \
        .option("user", user) \
        .option("password", password) \
        .option("fetchsize", 10000) \
        .load()

# Load only recent 90-day data for performance
bookings = load_redshift_table("(SELECT * FROM curated.bookings WHERE booking_date >= CURRENT_DATE - INTERVAL '90 day') AS recent_bookings").cache()
apartments = load_redshift_table("curated.apartments").cache()
apartment_attrs = load_redshift_table("curated.apartment_attributes").cache()

### KPI 1: Weekly Average Listing Price
weekly_avg_price = apartments \
    .withColumn("week", date_trunc("week", col("listing_created_on"))) \
    .groupBy("week") \
    .agg(avg("price").alias("avg_price")) \
    .coalesce(1)

weekly_avg_price.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "presentation.weekly_avg_price") \
    .option("user", user) \
    .option("password", password) \
    .option("tempdir", temp_dir) \
    .mode("overwrite") \
    .save()

### KPI 2: Repeat Customer Rate
repeat_customers = bookings \
    .withColumn("days_ago", datediff(current_date(), col("booking_date"))) \
    .filter(col("days_ago") <= 90) \
    .groupBy("user_id") \
    .agg(count("*").alias("total_bookings")) \
    .withColumn("is_repeat_customer", when(col("total_bookings") > 1, 1).otherwise(0)) \
    .coalesce(1)

repeat_customers.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "presentation.repeat_customers") \
    .option("user", user) \
    .option("password", password) \
    .option("tempdir", temp_dir) \
    .mode("overwrite") \
    .save()

### KPI 3: Most Popular Locations
popular_locations = bookings \
    .join(apartment_attrs, bookings.apartment_id == apartment_attrs.id) \
    .groupBy("cityname") \
    .agg(count("*").alias("num_bookings")) \
    .orderBy(col("num_bookings").desc()) \
    .coalesce(1)

popular_locations.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "presentation.popular_locations") \
    .option("user", user) \
    .option("password", password) \
    .option("tempdir", temp_dir) \
    .mode("overwrite") \
    .save()

### KPI 4: Occupancy Rate
occupancy = bookings \
    .withColumn("booked_nights", datediff(col("checkout_date"), col("checkin_date"))) \
    .groupBy(date_trunc("month", col("checkin_date")).alias("month")) \
    .agg(
        _sum("booked_nights").alias("total_booked_nights"),
        count("*").alias("num_bookings")
    ) \
    .withColumn("occupancy_rate", col("total_booked_nights") / (col("num_bookings") * 30.0)) \
    .coalesce(1)

occupancy.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "presentation.occupancy_rate") \
    .option("user", user) \
    .option("password", password) \
    .option("tempdir", temp_dir) \
    .mode("overwrite") \
    .save()

### KPI 5: Average Booking Duration
avg_duration = bookings \
    .withColumn("duration", datediff(col("checkout_date"), col("checkin_date"))) \
    .groupBy() \
    .agg(avg("duration").alias("avg_booking_duration")) \
    .coalesce(1)

avg_duration.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "presentation.avg_booking_duration") \
    .option("user", user) \
    .option("password", password) \
    .option("tempdir", temp_dir) \
    .mode("overwrite") \
    .save()

### KPI 6: Top Performing Listings
top_listings = bookings \
    .groupBy("apartment_id") \
    .agg(_sum("total_price").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .coalesce(1)

top_listings.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "presentation.top_performing_listings") \
    .option("user", user) \
    .option("password", password) \
    .option("tempdir", temp_dir) \
    .mode("overwrite") \
    .save()

job.commit()
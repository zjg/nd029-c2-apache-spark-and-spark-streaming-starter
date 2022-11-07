from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

stediSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", DoubleType()),
    StructField("riskDate", DateType()),
])

# : using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
spark = SparkSession.builder \
    .appName("balance-events") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

eventsRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()
# eventsRawDF.writeStream.format("console").start().awaitTermination()

# : cast the value column in the streaming dataframe as a STRING 
valueDF = eventsRawDF.select(col("value").cast("string"))
# valueDF.writeStream.format("console").start().awaitTermination()

# : parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
riskDF = valueDF \
    .withColumn("value", from_json(valueDF.value, stediSchema)) \
    .select([col(f"value.{name}") for name in stediSchema.fieldNames()])
# riskDF.writeStream.format("console").start().awaitTermination()
riskDF.createOrReplaceTempView("CustomerRisk")

# : execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
# : sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----

customerRiskStreamingDF = spark.sql("""
    select customer, score
    from CustomerRisk
    where isnotnull(customer)
""")

customerRiskStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 



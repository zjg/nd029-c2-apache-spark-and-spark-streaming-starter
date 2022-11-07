from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

# : create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
redisSchema = StructType([
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType()),
            ])
        )),
])

# : create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", DateType()),
])

# : create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
stediSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", DoubleType()),
    StructField("riskDate", DateType()),
])

#: create a spark application object
spark = SparkSession.builder \
    .appName("balance-events") \
    .getOrCreate()

#: set the spark log level to WARN
spark.sparkContext.setLogLevel('WARN')

# : using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redisRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# : cast the value column in the streaming dataframe as a STRING 
redisDF = redisRawDF \
    .selectExpr("cast(key as string) as key", "cast(value as string) as value")
# redisDF.writeStream.format("console").start()

# :; parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
customerEncodedDF = redisDF.withColumn("value", from_json(redisDF.value, redisSchema)) \
    .select(col("value.key"), col("value.zSetEntries"))
customerEncodedDF.createOrReplaceTempView("RedisSortedSet")
# spark.sql("select * from RedisSortedSet").writeStream.format("console").start()

# : execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
encodedCustomerDF = spark.sql("""
    select zSetEntries[0].element as encodedCustomer
    from RedisSortedSet
    where key = base64("Customer")
""")
# encodedCustomerDF.writeStream.format("console").start()

# : take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
jsonCustomerDF = encodedCustomerDF.withColumn("customer", unbase64(encodedCustomerDF.encodedCustomer).cast("string"))
# jsonCustomerDF.writeStream.format("console").start()

# : parse the JSON in the Customer record and store in a temporary view called CustomerRecords
customerDF = jsonCustomerDF \
    .withColumn("customer", from_json(jsonCustomerDF.customer, customerSchema)) \
    .select(col("customer"))
customerDF.createOrReplaceTempView("CustomerRecords")
# customerDF.writeStream.format("console").start()

# : JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql("""
    select customer.email, customer.birthDay
    from CustomerRecords
    where isnotnull(customer)
""")
# emailAndBirthDayStreamingDF.writeStream.format("console").start()

# : from the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)
# : Split the birth year as a separate field from the birthday
# : Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .withColumn("email", emailAndBirthDayStreamingDF.email) \
    .withColumn("birthYear", split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0)) \
    .select("email", "birthYear")

# : sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
# 
# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----

emailAndBirthYearStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct 
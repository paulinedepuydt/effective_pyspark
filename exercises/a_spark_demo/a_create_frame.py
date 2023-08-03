"""
Illustrate several ways to create small, toy-example dataframes.
This is incredibly useful in tests.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.getOrCreate()

fields = [
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
]
users = spark.createDataFrame(
    data=[
        ("Wim", 1),
        (None, 2),
    ],
    schema=StructType(fields),
)
users.show()
# +----+---+
# |name|age|
# +----+---+
# | Wim|  1|
# |null|  2|
# +----+---+
users.printSchema()
# root
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
users.registerTempTable("foo")
spark.sql("""
    SELECT 
        name,
        age * -1 as neg_age 
    FROM foo
""").show()
# +----+-------+
# |name|neg_age|
# +----+-------+
# | Wim|     -1|
# |null|     -2|
# +----+-------+

# A shorter way, with implicit assumptions: Spark will attempt to infer the datatypes.
# They will typically be chosen overly large.
currencies = spark.createDataFrame(
    data=[
        ("Euro", 1.0, 1),
        ("USD", 1.2, 1),
    ],
    schema=("currency", "value", "random"),
)

for frame in (users, currencies):
    frame.show()  # An action.
    frame.printSchema()  # Not an action.

import pyspark.sql.functions as sf

df1 = spark.range(3)
df2 = df1.withColumn("foo", sf.lit("bar"))
df3 = df2.withColumn("bar", sf.col("id") < 2)
df4 = df3.filter(sf.col("id") != 1)
df5 = df4.select("foo", "bar", "id", (sf.col("id") + 2).alias("plus2"))
df6 = df5.drop("id")
df7 = df6.withColumnRenamed("bar", "lessthan2")

df7.printSchema()
# root
#  |-- foo: string (nullable = false)
#  |-- lessthan2: boolean (nullable = false)
#  |-- plus2: long (nullable = false)
print(df7.count())
# 2
df7.show()
# +---+---------+-----+
# |foo|lessthan2|plus2|
# +---+---------+-----+
# |bar|     true|    2|
# |bar|    false|    4|
# +---+---------+-----+

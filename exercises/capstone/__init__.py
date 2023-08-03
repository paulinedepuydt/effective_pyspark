from pyspark import SparkConf
from pyspark.sql import SparkSession

BUCKET = "dmacademy-course-assets"
KEY = "vlerick/pre_release.csv"
config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.csv(f"s3a://{BUCKET}/{KEY}", header=True)


df.show()
print(df.toPandas())
df.select("budget", "language").repartition(1).write.json(
    f"s3a://{BUCKET}/vlerick/oliver", mode="overwrite"
)


# The magic committer impvoes writing performance on S3 for common serialization types. Iceberg and delta don't benefit.
# See: https://spot.io/blog/improve-apache-spark-performance-with-the-s3-magic-committer/
# "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": "true",

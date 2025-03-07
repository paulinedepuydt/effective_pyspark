# Exercise:
# “clean” a CSV file using PySpark.
# * Grab sample data from https://packages.revolutionanalytics.com/datasets/AirlineSubsetCsv.tar.gz
#   A copy of the data can be found on our S3 bucket (link shared in class).
# * Inspect the columns: what type of data do they hold?
# * Create an ETL job with PySpark where you read in the csv file, and perform
#   the cleansing steps mentioned in the classroom:
#   - improve column names (subjective)
#   - fix data types
#   - flag missing or unknown data
#   - remove redundant data
# * Write the data to a parquet file. How big is the parquet file compared
#   to the compressed csv file? And compared to the uncompressed csv file?
#   How long does your processing take?
# * While your job is running, open up the Spark UI and get a feel for what's
#   there (together with the instructor).
# For explanations on the columns, check https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as psf
from pyspark.sql.functions import expr

def read_data(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.csv(
        str(path),
        # For a CSV, `inferSchema=False` means every column stays of the string
        # type. There is no time wasted on inferring the schema, which is
        # arguably not something you would depend on in production either.
        inferSchema=False,
        header=True,
        # The dataset mixes two values for null: sometimes there's an empty attribute,
        # which you will see in the CSV file as two neighboring commas. But there are
        # also literal "null" strings, like in this sample: `420.0,null,,1.0,`
        # The following option makes a literal null-string equivalent to the empty value.
        nullValue="null",
    )


# use relative paths, so that the location of this project on your system
# won't mean editing paths
resources_dir = Path("/workspace/effective_pyspark/exercises") / "resources"
# Create the folder where the results of this script's ETL-pipeline will
# be stored.
# Extract
frame = read_data(resources_dir / "flight" / "part-00001")
frame.show()


def clean(df: DataFrame) -> DataFrame:
    # rename columns
    df = (df.withColumnRenamed("FL_DATE", "FLIGHT_DATE")
          .withColumnRenamed("ORIGIN_STATE_ABR", "ORIGIN_STATE")
          .withColumnRenamed("DEST_STATE_ABR", "DEST_STATE")
          .withColumnRenamed("DEP_TIME", "DEPARTURE_TIME")
          .withColumnRenamed("CRS_DEP_TIME", "PLANNED_DEPARTURE_TIME"))
    # change types
    # df = df.select(psf.to_date(psf.col("FLIGHT_DATE"), "dd-MMM-yyyy").alias("FLIGHT_DATE"))
    # drop columns
    df = df.drop("_c44")
    # filter NULLs
    df = df.na.drop(subset=["TAIL_NUM"])
    return df


# Transform
cleaned_frame = clean(frame)
cleaned_frame.show(4)

cleaned_frame.printSchema()
cleaned_frame.filter(cleaned_frame.TAIL_NUM.isNull()).show()


# Query
cleaned_frame.registerTempTable("table")

spark = SparkSession.builder.getOrCreate()
spark.sql("SELECT count(*) FROM table where YEAR=2011").show()


# Load
target_dir = Path("/workspace/effective_pyspark/exercises") / "target"
target_dir.mkdir(exist_ok=True)
cleaned_frame.write.parquet(
    path=str(target_dir / "cleaned_flights"),
    mode="overwrite",
    # Exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
    compression="snappy",
)


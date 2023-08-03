import datetime as dt

from pyspark.sql import Row, SparkSession
import pyspark.sql.functions as sf

r = Row(id=1, foo=True)

assert r.id == r["id"]

print("foo %s" % list((r, r)))


def assert_departments_have_a_realistic_start_date(df: DataFrame) -> None:
    bad_examples = (
        df.filter(
            df["start_date"].isNotNull()
            & (df["start_date"] > dt.date.today() + dt.timedelta(days=365 * 5))
        )
        .select("start_date")
        .take(5)
    )
    if bad_examples:
        raise RuntimeError(
            "Non-realistic start dates in org unit table: %s" % bad_examples
        )

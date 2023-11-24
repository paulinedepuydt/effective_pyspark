import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
import holidays
from pyspark.sql.types import BooleanType


def is_belgian_holiday(date: datetime.date) -> bool:
    return(date in holidays.BE())



def label_weekend(frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend") -> DataFrame:
    """Adds a column indicating whether or not the attribute `colname`
    in the corresponding row is a weekend day."""
    def is_weekend(my_date: datetime.date):
        weekday = my_date.weekday()
        return weekday >= 5

    udf_weekend = udf(lambda z: is_weekend(z), BooleanType())
    frame = frame.withColumn(new_colname, udf_weekend(col(colname)))
    return frame


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday."""
    pass


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass

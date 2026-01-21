from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class ChargePointsETLJob:
    input_path = "data/input/electric-chargepoints-2017.csv"
    output_path = "data/output/chargepoints-2017-analysis"

    def __init__(self):
        self.spark_session = (
            SparkSession.builder
            .master("local[*]")
            .appName("ElectricChargePointsETLJob")
            .getOrCreate()
        )

    def extract(self):
        """
        Read raw CSV data into a Spark DataFrame
        """
        return (
            self.spark_session.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(self.input_path)
        )

    def transform(self, df):
        """
        Aggregate plugin duration by charge point
        and round values to 2 decimal places
        """
        return (
            df.groupBy(F.col("CPID").alias("chargepoint_id"))
            .agg(
                F.round(F.max(F.col("PluginDuration")), 2).alias("max_duration"),
                F.round(F.avg(F.col("PluginDuration")), 2).alias("avg_duration")
            )
        )

    def load(self, df):
        """
        Save transformed data as Parquet
        """
        df.write.mode("overwrite").parquet(self.output_path)

    def run(self):
        """
        Run the ETL pipeline: extract -> transform -> load
        """
        self.load(self.transform(self.extract()))


if __name__ == "__main__":
    job = ChargePointsETLJob()
    job.run()

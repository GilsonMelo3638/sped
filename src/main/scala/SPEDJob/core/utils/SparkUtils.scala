package SPEDJob.core.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("yarn")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.executor.instances", "20")
      .getOrCreate()
  }
}
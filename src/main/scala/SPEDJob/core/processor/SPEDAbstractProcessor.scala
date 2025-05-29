package SPEDJob.core.processor

import SPEDJob.models.SPEDConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class SPEDAbstractProcessor(spark: SparkSession) {

  protected def readInput(inputPath: String): DataFrame = {
    spark.read.parquet(inputPath)
  }

  protected def processData(
                             inputDF: DataFrame,
                             referenceData: Map[String, DataFrame],
                             query: String
                           ): DataFrame = {
    inputDF.createOrReplaceTempView("input_reg")
    referenceData.foreach { case (name, df) => df.createOrReplaceTempView(name) }
    spark.sql(query)
  }

  protected def writeOutput(outputDF: DataFrame, outputPath: String): Unit = {
    val targetFileSizeMB = 512
    val maxFilesPerPeriodo = 10
    val bytesPerMB = 1024 * 1024
    val targetBytesPerFile = targetFileSizeMB * bytesPerMB

    // Estimar quantidade de linhas por PERIODO_SPED_BASE
    val rowCountsDF = outputDF.groupBy("PERIODO_SPED_BASE").count()

    // Aproximar o tamanho da linha em bytes (sem sample, mais leve)
    val approxRowSizeBytes = 500L  // valor conservador ajustável

    // Juntar contagem com o DataFrame original
    val dfWithCount = outputDF
      .join(rowCountsDF, Seq("PERIODO_SPED_BASE"))
      .withColumn("estimated_size_bytes", col("count") * approxRowSizeBytes)
      .withColumn("num_files", greatest(lit(1), least(lit(maxFilesPerPeriodo),
        ceil(col("estimated_size_bytes") / lit(targetBytesPerFile)).cast("int")
      )))
      // Distribuição de arquivos de forma uniforme
      .withColumn("partition_id", spark_partition_id())
      .withColumn("file_split", (monotonically_increasing_id() % col("num_files")).cast("int"))

    // Reparticionar pelos splits + PERIODO
    dfWithCount
      .repartition(col("PERIODO_SPED_BASE"), col("file_split"))
      .drop("count", "estimated_size_bytes", "num_files", "partition_id", "file_split")
      .write
      .mode("overwrite")
      .option("compression", "lz4")
      .option("parquet.block.size", targetBytesPerFile)
      .partitionBy("PERIODO_SPED_BASE")
      .parquet(outputPath)
  }


  def process(
               referenceData: Map[String, DataFrame],
               config: SPEDConfig
             ): DataFrame = {
    val inputDF = readInput(config.inputPath)
    val processedDF = processData(inputDF, referenceData, config.query)
    writeOutput(processedDF, config.outputPath)
    processedDF
  }
}
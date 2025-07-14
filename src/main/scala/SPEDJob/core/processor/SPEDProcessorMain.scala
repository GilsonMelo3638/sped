package SPEDJob.core.processor

import SPEDJob.core.utils.ExternalTableCreator
import SPEDJob.processors._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object SPEDProcessorMain {
  // Lista de tabelas a serem processadas (centralizada para evitar repetição)
  private val TABLES_TO_PROCESS = List(
    "0150", "0100", "0200", "0220", "0221", "0450", "0460", "9900", "9999", "B020", "B025",
    "B030", "B035", "B350", "B420", "B440", "B460", "B470", "B500", "B510", "C100", "C113",
    "C170", "C176", "C190", "C191", "C195", "C197", "C390", "C400", "C405", "C410", "C420",
    "C460", "C470", "C490", "C500", "C510", "C590", "C595", "D100", "D101", "D190", "D195",
    "D197", "D300", "D310", "D400", "D410", "D420", "D500", "D510", "D530", "D590", "D695",
    "D696", "D697", "E100", "E110", "E111", "E112", "E113", "E115", "E116", "E200", "E210",
    "E220", "E230", "E240", "E250", "E300", "E310", "E311", "E312", "E313", "E316", "E500",
    "E510", "E520", "E530", "E531"
  )

  // Mapeamento de processadores (centralizado)
  private val PROCESSORS = Map(
    "0150" -> SPED0150Processor.config,
    "0200" -> SPED0200Processor.config,
    "0100" -> SPED0100Processor.config,
    "0220" -> SPED0220Processor.config,
    "0221" -> SPED0221Processor.config,
    "0450" -> SPED0450Processor.config,
    "0460" -> SPED0460Processor.config,
    "9900" -> SPED9900Processor.config,
    "9999" -> SPED9999Processor.config,
    "B020" -> SPEDB020Processor.config,
    "B025" -> SPEDB025Processor.config,
    "B030" -> SPEDB030Processor.config,
    "B035" -> SPEDB035Processor.config,
    "B350" -> SPEDB350Processor.config,
    "B420" -> SPEDB420Processor.config,
    "B440" -> SPEDB440Processor.config,
    "B460" -> SPEDB460Processor.config,
    "B470" -> SPEDB470Processor.config,
    "B500" -> SPEDB500Processor.config,
    "B510" -> SPEDB510Processor.config,
    "C100" -> SPEDC100Processor.config,
    "C113" -> SPEDC113Processor.config,
    "C170" -> SPEDC170Processor.config,
    "C176" -> SPEDC176Processor.config,
    "C190" -> SPEDC190Processor.config,
    "C191" -> SPEDC191Processor.config,
    "C195" -> SPEDC195Processor.config,
    "C197" -> SPEDC197Processor.config,
    "C390" -> SPEDC390Processor.config,
    "C400" -> SPEDC400Processor.config,
    "C405" -> SPEDC405Processor.config,
    "C410" -> SPEDC410Processor.config,
    "C420" -> SPEDC420Processor.config,
    "C460" -> SPEDC460Processor.config,
    "C470" -> SPEDC470Processor.config,
    "C490" -> SPEDC490Processor.config,
    "C500" -> SPEDC500Processor.config,
    "C510" -> SPEDC510Processor.config,
    "C590" -> SPEDC590Processor.config,
    "C595" -> SPEDC595Processor.config,
    "D100" -> SPEDD100Processor.config,
    "D101" -> SPEDD101Processor.config,
    "D190" -> SPEDD190Processor.config,
    "D195" -> SPEDD195Processor.config,
    "D197" -> SPEDD197Processor.config,
    "D300" -> SPEDD300Processor.config,
    "D310" -> SPEDD310Processor.config,
    "D400" -> SPEDD400Processor.config,
    "D410" -> SPEDD410Processor.config,
    "D420" -> SPEDD420Processor.config,
    "D500" -> SPEDD500Processor.config,
    "D510" -> SPEDD510Processor.config,
    "D530" -> SPEDD530Processor.config,
    "D590" -> SPEDD590Processor.config,
    "D695" -> SPEDD695Processor.config,
    "D696" -> SPEDD696Processor.config,
    "D697" -> SPEDD697Processor.config,
    "E100" -> SPEDE100Processor.config,
    "E110" -> SPEDE110Processor.config,
    "E111" -> SPEDE111Processor.config,
    "E112" -> SPEDE112Processor.config,
    "E113" -> SPEDE113Processor.config,
    "E115" -> SPEDE115Processor.config,
    "E116" -> SPEDE116Processor.config,
    "E200" -> SPEDE200Processor.config,
    "E210" -> SPEDE210Processor.config,
    "E220" -> SPEDE220Processor.config,
    "E230" -> SPEDE230Processor.config,
    "E240" -> SPEDE240Processor.config,
    "E250" -> SPEDE250Processor.config,
    "E300" -> SPEDE300Processor.config,
    "E310" -> SPEDE310Processor.config,
    "E311" -> SPEDE311Processor.config,
    "E312" -> SPEDE312Processor.config,
    "E313" -> SPEDE313Processor.config,
    "E316" -> SPEDE316Processor.config,
    "E500" -> SPEDE500Processor.config,
    "E510" -> SPEDE510Processor.config,
    "E520" -> SPEDE520Processor.config,
    "E530" -> SPEDE530Processor.config,
    "E531" -> SPEDE531Processor.config
  )

  def setupSpark(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("yarn")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.executor.instances", "20")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic") // Importante para overwrite parcial
      .getOrCreate()
  }

  def processInitialData(spark: SparkSession): (DataFrame, Map[String, DataFrame]) = {
    // 1. Processar tabelas principais
    val r0000 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_0000")
    val sped_base = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_BASE")

    // 2. Criar views temporárias
    r0000.createOrReplaceTempView("r0000")
    sped_base.createOrReplaceTempView("sped_base")

    // 3. Processar junção inicial
    val r0000xsped_base = spark.sql(
      """
      SELECT
        c.*,
        s.PERIODO,
        s.ULTIMA_EFD,
        CAST((s.DATAHORA_PROCESSAMENTO / 1000) AS TIMESTAMP) AS DATAHORA_PROCESSAMENTO,
        s.STATUS_PROCESSAMENTO,
        CONCAT(SUBSTRING(CAST(c.DT_INI AS STRING), 5, 4), SUBSTRING(CAST(c.DT_INI AS STRING), 3, 2)) AS PERIODO_0000
      FROM sped_base s
      INNER JOIN r0000 c ON s.id = c.id_base
      WHERE s.status_processamento IN ('10', '12', '13', '14')
      """
    )

    // 4. Escrever e cachear resultado
    r0000xsped_base
      .repartition(col("PERIODO")) // Força uma partição por valor de PERIODO
      .write
      .mode("overwrite")
      .option("compression", "lz4") // Compactação LZ4
      .partitionBy("PERIODO") // Estrutura de pastas particionadas
      .parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0000")
    val r0000Cached = spark.read.parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0000").cache()

    // 5. Carregar todas as tabelas necessárias
    val referenceData = TABLES_TO_PROCESS.map { table =>
      val df = spark.read.parquet(s"/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_$table")
      df.createOrReplaceTempView(s"r$table")
      (s"r$table", df)
    }.toMap

    (r0000Cached, referenceData)
  }

  def main(args: Array[String]): Unit = {
    val spark = setupSpark("SPED Processor")

    // Carregar todos os dados de referência
    val (r0000Cached, referenceData) = processInitialData(spark)

    // Processar todas as tabelas na ordem definida
    TABLES_TO_PROCESS.foreach { processorKey =>
      PROCESSORS.get(processorKey).foreach { config =>
        val processor = new SPEDAbstractProcessor(spark) {}
        processor.process(referenceData + ("r0000" -> r0000Cached), config)
      }
    }
    ExternalTableCreator.main(Array())
    spark.stop()
  }
}
//SPEDProcessorMain.main(Array())
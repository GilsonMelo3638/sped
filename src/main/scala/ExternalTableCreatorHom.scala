import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExternalTableCreatorHom {
  // Configuração de autenticação LDAP para Hive
  private val ldapUsername = "svc_bigdata"
  private val ldapPassword = "@svc.bigdata1"

  // Cria uma configuração de Spark
  private val conf = new SparkConf()
    .set("spark.hadoop.hive.server2.authentication", "LDAP")
    .set("spark.hadoop.hive.server2.authentication.ldap.url", "ldap://fazenda.net:389")
    .set("spark.hadoop.hive.server2.authentication.ldap.baseDN", "OU=USUARIOS,DC=fazenda,DC=net")
    .set("spark.hadoop.hive.server2.custom.authentication.username", ldapUsername)
    .set("spark.hadoop.hive.server2.custom.authentication.password", ldapPassword)

  // Cria o SparkSession com suporte ao Hive
  private val spark = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  def createExternalTables(documento: String, parquetPath: String, tables: List[String]): Unit = {
    tables.foreach { table =>
      val df = spark.read.parquet(s"$parquetPath/$table")
      val schemaWithoutPartition = df.schema.filter(_.name != "PERIODO_SPED_BASE")
      val schemaSql = schemaWithoutPartition.map(field => s"`${field.name}` ${field.dataType.simpleString.toUpperCase}").mkString(",\n")
      val partitionColumn = "PERIODO_SPED_BASE STRING"

      val createTableSql =
        s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS seec_prdc_declaracao_fiscal.${documento}_$table (
          $schemaSql
      )
      PARTITIONED BY ($partitionColumn)
      STORED AS PARQUET
      LOCATION '${parquetPath}/$table'
      """

      spark.sql(createTableSql)
      spark.sql(s"MSCK REPAIR TABLE seec_prdc_declaracao_fiscal.${documento}_$table")
      spark.sql(s"REFRESH TABLE seec_prdc_declaracao_fiscal.${documento}_$table")
    }
  }

  def dropExternalTables(documento: String, tables: List[String]): Unit = {
    tables.foreach { table =>
      spark.sql(s"DROP TABLE IF EXISTS seec_prdc_declaracao_fiscal.${documento}_$table")
      println(s"Tabela ${documento}_$table excluída com sucesso.")
    }
  }

  def main(args: Array[String]): Unit = {
    val datasets = List(
      ("sped", "hdfs:///datalake/prata/sources/dbms/ADMSPED", List(
        "REG_9900",
        "REG_B030",
        "REG_B035",
        "REG_B350",
        "REG_B420",
        "REG_B460",
        "REG_B500",
        "REG_B510",
        "REG_C113",
        "REG_C191",
        "REG_C390",
        "REG_C400",
        "REG_C405",
        "REG_C410",
        "REG_C420",
        "REG_C460",
        "REG_C470",
        "REG_C490",
        "REG_C500",
        "REG_C510",
        "REG_C590",
        "REG_C595",
        "REG_D101",
        "REG_D195",
        "REG_D197",
        "REG_D300",
        "REG_D310",
        "REG_D400",
        "REG_D410",
        "REG_D420",
        "REG_D500",
        "REG_D510",
        "REG_D530",
        "REG_D590",
        "REG_D695",
        "REG_D696",
        "REG_D697",
        "REG_E200",
        "REG_E300",
        "REG_E311",
        "REG_E313",
        "REG_E316",
        "REG_E500",
        "REG_E510",
        "REG_E520",
        "REG_E530",
        "REG_E531"
      ))
    )

    // Para criar as tabelas
    datasets.foreach { case (documento, parquetPath, tables) =>
      createExternalTables(documento, parquetPath, tables)
    }

    // Para excluir as tabelas (descomente se necessário)
    // datasets.foreach { case (documento, _, tables) =>
    //   dropExternalTables(documento, tables)
    // }
  }
}

//ExternalTableCreatorHom.main(Array())
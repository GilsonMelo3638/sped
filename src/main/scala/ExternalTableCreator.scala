import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExternalTableCreator {
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
        "REG_0150"
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

//ExternalTableCreator.main(Array())
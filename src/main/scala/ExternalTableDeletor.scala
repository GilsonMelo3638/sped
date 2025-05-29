//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//
//// Configuração de autenticação LDAP para Hive (mesma do script original)
//val ldapUsername = "svc_bigdata"
//val ldapPassword = "@svc.bigdata1"
//
//// Cria uma configuração de Spark (mesma do script original)
//val conf = new SparkConf()
//conf.set("spark.hadoop.hive.server2.authentication", "LDAP")
//conf.set("spark.hadoop.hive.server2.authentication.ldap.url", "ldap://fazenda.net:389")
//conf.set("spark.hadoop.hive.server2.authentication.ldap.baseDN", "OU=USUARIOS,DC=fazenda,DC=net")
//conf.set("spark.hadoop.hive.server2.custom.authentication.username", ldapUsername)
//conf.set("spark.hadoop.hive.server2.custom.authentication.password", ldapPassword)
//
//// Cria o SparkSession com suporte ao Hive (mesma do script original)
//val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//
//// Lista de tabelas (mesma do script original)
//val tables = List(
//  "REG_0150", "REG_0200", "REG_0220", "REG_0221",
//  "REG_0450", "REG_0460", "REG_B020", "REG_B025", "REG_B440",
//  "REG_B470", "REG_C100", "REG_C170", "REG_C176", "REG_C190",
//  "REG_C195", "REG_C197", "REG_D100", "REG_D190", "REG_E110",
//  "REG_E111", "REG_E112", "REG_E113", "REG_E115", "REG_E116",
//  "REG_E210", "REG_E220", "REG_E230", "REG_E240", "REG_E250",
//  "REG_E310"
//)
//
//// Comandos para excluir cada tabela externa
//tables.foreach { table =>
//  val dropTableSql = s"DROP TABLE IF EXISTS seec_prdc_declaracao_fiscal.sped_$table"
//  spark.sql(dropTableSql)
//  println(s"Tabela sped_$table excluída com sucesso.")
//}
//
//spark.close()
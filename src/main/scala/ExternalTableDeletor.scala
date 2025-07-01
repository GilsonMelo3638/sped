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
//  "REG_0150", "REG_0200", "REG_0220", "REG_0221", "REG_0450",
//  "REG_0460", "REG_B020", "REG_B025", "REG_B030", "REG_B035",
//  "REG_B350", "REG_B420", "REG_B440", "REG_B460", "REG_B470",
//  "REG_B500", "REG_B510", "REG_C100", "REG_C113", "REG_C170",
//  "REG_C176", "REG_C190", "REG_C191", "REG_C195", "REG_C197",
//  "REG_C390", "REG_C400", "REG_C405", "REG_C410", "REG_C420",
//  "REG_C460", "REG_C470", "REG_C490", "REG_C500", "REG_C510",
//  "REG_C590", "REG_C595", "REG_D100", "REG_D101", "REG_D190",
//  "REG_D195", "REG_D197", "REG_D300", "REG_D310", "REG_D400",
//  "REG_D410", "REG_D420", "REG_D500", "REG_D510", "REG_D530",
//  "REG_D590", "REG_D695", "REG_D696", "REG_D697", "REG_E110",
//  "REG_E111", "REG_E112", "REG_E113", "REG_E115", "REG_E116",
//  "REG_E200", "REG_E210", "REG_E220", "REG_E230", "REG_E240",
//  "REG_E250", "REG_E300", "REG_E310", "REG_E311", "REG_E313",
//  "REG_E316", "REG_E500", "REG_E510", "REG_E520", "REG_E530",
//  "REG_E531", "REG_9900", "REG_9999"
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
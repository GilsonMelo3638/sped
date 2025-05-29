package SPEDJob.core.processor

import SPEDJob.models.SPEDConfig

trait SPEDBaseProcessor {
  // Método abstrato que cada processador deve implementar
  def registro: String

  // Métodos para gerar caminhos automaticamente
  protected def inputPath: String =
    s"/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_$registro"

  protected def outputPath: String =
    s"/datalake/prata/sources/dbms/ADMSPED/REG_$registro"

  // Método para criar a configuração
  def config(query: String): SPEDConfig =
    SPEDConfig(inputPath, outputPath, query)
}
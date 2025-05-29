package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPED0150Processor extends SPEDBaseProcessor {
  override def registro: String = "0150"

  val config: SPEDConfig = super.config(
  """
      SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          s.COD_PART,
          s.NOME,
          LPAD(CAST(s.COD_PAIS AS STRING), 5, '0') AS COD_PAIS,
          LPAD(CAST(s.CNPJ AS STRING), 14, '0') AS CNPJ,
          LPAD(CAST(s.CPF AS STRING), 11, '0') AS CPF,
          s.IE,
          LPAD(CAST(s.COD_MUN AS STRING), 7, '0') AS COD_MUN,
          s.SUFRAMA,
          s.END,
          s.NUM,
          s.COMPL,
          s.BAIRRO,
          c.ULTIMA_EFD,
          c.PERIODO_0000,
          c.CPF AS CPF_0000,
          c.CNPJ AS CNPJ_0000,
          c.IE AS IE_0000,
          c.PERIODO AS PERIODO_SPED_BASE
      FROM input_reg s
      INNER JOIN r0000 c ON s.id_base = c.id_base
    """
  )
}
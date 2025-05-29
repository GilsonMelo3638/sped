package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPED0200Processor extends SPEDBaseProcessor {
  override def registro: String = "0200"

  val config: SPEDConfig = super.config(
  """
      SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          s.COD_ITEM,
          s.DESCR_ITEM,
          s.COD_BARRA,
          s.COD_ANT_ITEM,
          s.UNID_INV,
          CAST(s.TIPO_ITEM AS INT) AS TIPO_ITEM,
          LPAD(CAST(s.COD_NCM AS STRING), 8, '0') AS COD_NCM,
          s.EX_IPI,
          LPAD(CAST(s.COD_GEN AS STRING), 2, '0') AS COD_GEN,
          s.COD_LST,
          CAST(s.ALIQ_ICMS AS DECIMAL(4, 2)) AS ALIQ_ICMS,
          LPAD(CAST(s.CEST AS STRING), 2, '0') AS CEST,
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
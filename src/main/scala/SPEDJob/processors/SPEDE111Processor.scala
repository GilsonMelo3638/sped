package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDE111Processor extends SPEDBaseProcessor {
  override def registro: String = "E111"
  val config: SPEDConfig = super.config(
    """
        SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          s.COD_AJ_APUR,
          s.DESCR_COMPL_AJ,
          CAST(s.VL_AJ_APUR AS DECIMAL(18, 2)) AS VL_AJ_APUR,
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
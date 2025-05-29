package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDE115Processor extends SPEDBaseProcessor {
  override def registro: String = "E115"
  val config: SPEDConfig = super.config(
    """
        SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          CAST(s.	VL_INF_ADIC AS DECIMAL(18, 2)) AS VL_INF_ADIC,
          s.DESCR_COMPL_AJ,
          s.COD_INF_ADIC,
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
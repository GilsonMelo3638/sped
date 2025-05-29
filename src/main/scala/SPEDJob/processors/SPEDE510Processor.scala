package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDE510Processor extends SPEDBaseProcessor {
  override def registro: String = "E510"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.CST_IPI,
        CAST(s.VL_CONT_IPI AS DECIMAL(18, 2)) AS VL_CONT_IPI,
        CAST(s.VL_BC_IPI AS DECIMAL(18, 2)) AS VL_BC_IPI,
        CAST(s.VL_IPI AS DECIMAL(18, 2)) AS VL_IPI,
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
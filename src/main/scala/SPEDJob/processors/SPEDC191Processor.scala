package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC191Processor extends SPEDBaseProcessor {
  override def registro: String = "C191"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        CAST(s.VL_FCP_OP AS DECIMAL(18, 2)) AS VL_FCP_OP,
        CAST(s.VL_FCP_ST as DECIMAL(18,2)) AS VL_FCP_ST,
        CAST(s.VL_FCP_RET AS DECIMAL(18, 2)) AS VL_FCP_RET,
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
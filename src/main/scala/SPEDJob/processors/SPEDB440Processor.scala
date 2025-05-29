package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDB440Processor extends SPEDBaseProcessor {
  override def registro: String = "B440"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.IND_OPER,
        s.COD_PART,
        CAST(s.VL_CONT_RT AS DECIMAL(18, 2)) AS VL_CONT_RT,
        CAST(s.VL_BC_ISS_RT AS DECIMAL(18, 2)) AS VL_BC_ISS_RT,
        CAST(s.VL_ISS_RT AS DECIMAL(18, 2)) AS VL_ISS_RT,
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
package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDB035Processor extends SPEDBaseProcessor {
  override def registro: String = "B035"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        CAST(s.VL_CONT_P AS DECIMAL(18, 2)) AS VL_CONT_P,
        CAST(s.VL_BC_ISS_P AS DECIMAL(18, 2)) AS VL_BC_ISS_P,
        CAST(s.ALIQ_ISS AS DECIMAL(4, 2)) AS ALIQ_ISS,
        CAST(s.VL_ISS_P AS DECIMAL(18, 2)) AS VL_ISS_P,
        CAST(s.VL_ISNT_ISS_P AS DECIMAL(18, 2)) AS VL_ISNT_ISS_P,
        s.COD_SERV,
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
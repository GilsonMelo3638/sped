package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDD590Processor extends SPEDBaseProcessor {
  override def registro: String = "D590"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.CST_ICMS,
        s.CFOP,
        CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
        CAST(s.VL_OPR AS DECIMAL(18, 2)) AS VL_OPR,
        CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
        CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
        CAST(s.VL_BC_ICMS_UF AS DECIMAL(18, 2)) AS VL_BC_ICMS_UF,
        CAST(s.VL_ICMS_UF AS DECIMAL(18, 2)) AS VL_ICMS_UF,
        CAST(s.VL_RED_BC AS DECIMAL(18, 2)) AS VL_RED_BC,
        s.COD_OBS,
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
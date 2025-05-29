package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDD190Processor extends SPEDBaseProcessor {
  override def registro: String = "D190"
  val config: SPEDConfig = super.config(
    """
        SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          CAST(s.CST_ICMS AS STRING) AS CST_ICMS,
          CAST(s.CFOP AS STRING) AS CFOP,
          CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
          CAST(s.VL_OPR AS DECIMAL(18, 2)) AS VL_OPR,
          CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
          CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
          CAST(s.VL_RED_BC AS DECIMAL(18, 2)) AS VL_RED_BC,
          CAST(s.COD_OBS AS STRING) AS COD_OBS,
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
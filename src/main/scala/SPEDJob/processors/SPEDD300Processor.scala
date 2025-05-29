package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDD300Processor extends SPEDBaseProcessor {
  override def registro: String = "D300"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.COD_MOD,
        s.SER,
        s.SUB,
        s.NUM_DOC_INI,
        s.NUM_DOC_FIN,
        s.CST_ICMS,
        s.CFOP,
        CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
        to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
        CAST(s.VL_OPR AS DECIMAL(18, 2)) AS VL_OPR,
        CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
        CAST(s.VL_SERV AS DECIMAL(18, 2)) AS VL_SERV,
        CAST(s.VL_SEG AS DECIMAL(18, 2)) AS VL_SEG,
        CAST(s.VL_OUT_DESP AS DECIMAL(18, 2)) AS VL_OUT_DESP,
        CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
        CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
        CAST(s.VL_RED_BC AS DECIMAL(18, 2)) AS VL_RED_BC,
        s.COD_OBS,
        s.COD_CTA,
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
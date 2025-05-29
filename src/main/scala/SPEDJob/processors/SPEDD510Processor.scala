package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDD510Processor extends SPEDBaseProcessor {
  override def registro: String = "D510"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.NUM_ITEM,
        s.COD_ITEM,
        s.COD_CLASS,
        s.QTD,
        s.UNID,
        CAST(s.VL_ITEM AS DECIMAL(18, 2)) AS VL_ITEM,
        CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
        s.CST_ICMS,
        s.CFOP,
        CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
        CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
        CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
        CAST(s.VL_BC_ICMS_UF AS DECIMAL(18, 2)) AS VL_BC_ICMS_UF,
        CAST(s.VL_ICMS_UF AS DECIMAL(18, 2)) AS VL_ICMS_UF,
        s.IND_REC,
        s.COD_PART,
        CAST(s.VL_PIS AS DECIMAL(18, 2)) AS VL_PIS,
        CAST(s.VL_COFINS AS DECIMAL(18, 2)) AS VL_COFINS,
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
package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC470Processor extends SPEDBaseProcessor {
  override def registro: String = "C470"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.COD_ITEM,
        s.QTD,
        s.QTD_CANC,
        s.UNID,
        CAST(s.VL_ITEM AS DECIMAL(18, 2)) AS VL_ITEM,
        s.CST_ICMS,
        s.CFOP,
        CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
        CAST(s.VL_PIS AS DECIMAL(18, 2)) AS VL_PIS,
        CAST(s.VL_COFINS AS DECIMAL(18, 2)) AS VL_COFINS,
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
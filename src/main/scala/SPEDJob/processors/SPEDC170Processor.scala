package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC170Processor extends SPEDBaseProcessor {
  override def registro: String = "C170"
  val config: SPEDConfig = super.config(
    """
      SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          LPAD(CAST(s.NUM_ITEM AS STRING), 3, '0') AS NUM_ITEM,
          s.COD_ITEM,
          s.DESCR_COMPL,
          CAST(s.QTD AS DECIMAL(18, 5)) AS QTD,
          s.UNID,
          CAST(s.VL_ITEM AS DECIMAL(18, 2)) AS VL_ITEM,
          CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
          s.IND_MOV,
          LPAD(CAST(s.CST_ICMS AS STRING), 3, '0') AS CST_ICMS,
          LPAD(CAST(s.CFOP AS STRING), 4, '0') AS CFOP,
          s.COD_NAT,
          CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
          CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
          CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
          CAST(s.VL_BC_ICMS_ST AS DECIMAL(18, 2)) AS VL_BC_ICMS_ST,
          CAST(s.ALIQ_ST AS DECIMAL(6, 2)) AS ALIQ_ST,
          CAST(s.VL_ICMS_ST AS DECIMAL(18, 2)) AS VL_ICMS_ST,
          s.IND_APUR,
          s.CST_IPI,
          s.COD_ENQ,
          CAST(s.VL_BC_IPI AS DECIMAL(18, 2)) AS VL_BC_IPI,
          CAST(s.ALIQ_IPI AS DECIMAL(6, 2)) AS ALIQ_IPI,
          CAST(s.VL_IPI AS DECIMAL(18, 2)) AS VL_IPI,
          LPAD(CAST(s.CST_PIS AS STRING), 3, '0') AS CST_PIS,
          CAST(s.VL_BC_PIS AS DECIMAL(18, 2)) AS VL_BC_PIS,
          CAST(s.ALIQ_PIS_PERCENT AS DECIMAL(8, 4)) AS ALIQ_PIS_PERCENT,
          CAST(s.QUANT_BC_PIS AS DECIMAL(18, 3)) AS QUANT_BC_PIS,
          CAST(s.ALIQ_PIS_REAIS AS DECIMAL(8, 4)) AS ALIQ_PIS_REAIS,
          CAST(s.VL_PIS AS DECIMAL(18, 2)) AS VL_PIS,
          LPAD(CAST(s.CST_COFINS AS STRING), 2, '0') AS CST_COFINS,
          CAST(s.VL_BC_COFINS AS DECIMAL(18, 2)) AS VL_BC_COFINS,
          CAST(s.ALIQ_COFINS_PERCENT AS DECIMAL(8, 8)) AS ALIQ_COFINS_PERCENT,
          CAST(s.QUANT_BC_COFINS AS Decimal(18, 3)) AS QUANT_BC_COFINS,
          CAST(s.ALIQ_COFINS_REAIS AS Decimal(8, 4)) AS ALIQ_COFINS_REAIS,
          CAST(s.VL_COFINS AS Decimal(18, 2)) AS VL_COFINS,
          CAST(s.VL_ABAT_NT AS Decimal(18, 2)) AS VL_ABAT_NT,
          s.COD_CTA,  -- Coluna adicionada da tabela C170
          c.ULTIMA_EFD,
          c.PERIODO_0000,
          c.CPF AS CPF_0000,
          c.CNPJ AS CNPJ_0000,
          c.IE AS IE_0000,
          c.PERIODO AS PERIODO_SPED_BASE,
          CAST(r.ALIQ_ICMS AS Decimal(6, 2)) AS ALIQ_ICMS_0200,
          r.COD_NCM  -- Coluna adicionada da tabela r0200
      FROM input_reg s
      INNER JOIN r0000 c ON s.id_base = c.id_base
      LEFT JOIN r0200 r ON s.id_base = r.id_base AND s.COD_ITEM = r.COD_ITEM
    """
  )
}
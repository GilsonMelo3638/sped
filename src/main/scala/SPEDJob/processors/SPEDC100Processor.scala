package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC100Processor extends SPEDBaseProcessor {
  override def registro: String = "C100"
  val config: SPEDConfig = super.config(
    """
      SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          s.IND_OPER,
          s.IND_EMIT,
          s.COD_PART,
          s.COD_MOD,
          s.COD_SIT,
          s.SER,
          s.NUM_DOC,
          s.CHV_NFE,
          to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
          to_date(CAST(s.DT_E_S AS STRING), 'ddMMyyyy') as DT_E_S,
          CAST(s.VL_DOC AS DECIMAL(18, 2)) AS VL_DOC,
          s.IND_PGTO,
          CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
          CAST(s.VL_ABAT_NT AS DECIMAL(18, 2)) AS VL_ABAT_NT,
          CAST(s.VL_MERC AS DECIMAL(18, 2)) AS VL_MERC,
          s.IND_FRT,
          CAST(s.VL_FRT AS DECIMAL(18, 2)) AS VL_FRT,
          CAST(s.VL_SEG AS DECIMAL(18, 2)) AS VL_SEG,
          CAST(s.VL_OUT_DA AS DECIMAL(18, 2)) AS VL_OUT_DA,
          CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
          CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
          CAST(s.VL_BC_ICMS_ST AS DECIMAL(18, 2)) AS VL_BC_ICMS_ST,
          CAST(s.VL_ICMS_ST AS DECIMAL(18, 2)) AS VL_ICMS_ST,
          CAST(s.VL_IPI AS DECIMAL(18, 2)) AS VL_IPI,
          CAST(s.VL_PIS AS DECIMAL(18, 2)) AS VL_PIS,
          CAST(s.VL_COFINS AS DECIMAL(18, 2)) AS VL_COFINS,
          CAST(s.VL_PIS_ST AS DECIMAL(18, 2)) AS VL_PIS_ST,
          CAST(s.VL_COFINS_ST AS DECIMAL(18, 2)) AS VL_COFINS_ST,
          c.ULTIMA_EFD,
          c.PERIODO_0000,
          c.CPF AS CPF_0000,
          c.CNPJ AS CNPJ_0000,
          c.IE AS IE_0000,
          c.PERIODO AS PERIODO_SPED_BASE,
          r.CPF AS CPF_0150,
          r.CNPJ AS CNPJ_0150
      FROM input_reg s
      INNER JOIN r0000 c ON s.id_base = c.id_base
      LEFT JOIN r0150 r ON s.id_base = r.id_base AND s.COD_PART = r.COD_PART
    """
  )
}
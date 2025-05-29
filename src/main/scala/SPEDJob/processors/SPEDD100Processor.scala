package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDD100Processor extends SPEDBaseProcessor {
  override def registro: String = "D100"
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
        CAST(s.COD_SIT AS INT) AS COD_SIT,
        s.SER,
        s.SUB,
        CAST(s.NUM_DOC AS BIGINT) AS NUM_DOC,
        CAST(s.CHV_CTE AS STRING) AS CHV_CTE,
        to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
        to_date(CAST(s.DT_A_P AS STRING), 'ddMMyyyy') as DT_A_P,
        CAST(s.TP_CT_E AS INT) AS TP_CT_E,
        CAST(s.CHV_CTE_REF AS STRING) AS CHV_CTE_REF,
        CAST(s.VL_DOC AS DECIMAL(18, 2)) AS VL_DOC,
        CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
        s.IND_FRT,
        CAST(s.VL_SERV AS DECIMAL(18, 2)) AS VL_SERV,
        CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
        CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
        CAST(s.VL_NT AS DECIMAL(18, 2)) AS VL_NT,
        s.COD_INF,
        s.COD_CTA,
        CAST(s.COD_MUN_ORIG AS INT) AS COD_MUN_ORIG,
        CAST(s.COD_MUN_DEST AS INT) AS COD_MUN_DEST,
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
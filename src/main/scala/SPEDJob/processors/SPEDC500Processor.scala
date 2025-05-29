package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC500Processor extends SPEDBaseProcessor {
  override def registro: String = "C500"
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
        s.SUB,
        s.COD_CONS,
        s.NUM_DOC,
        to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
        to_date(CAST(s.DT_E_S AS STRING), 'ddMMyyyy') as DT_E_S,
        CAST(s.VL_DOC AS DECIMAL(18, 2)) AS VL_DOC,
        CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
        CAST(s.VL_FORN AS DECIMAL(18, 2)) AS VL_FORN,
        CAST(s.VL_SERV_NT AS DECIMAL(18, 2)) AS VL_SERV_NT,
        CAST(s.VL_TERC AS DECIMAL(18, 2)) AS VL_TERC,
        CAST(s.VL_DA AS DECIMAL(18, 2)) AS VL_DA,
        CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
        CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
        CAST(s.VL_BC_ICMS_ST AS DECIMAL(18, 2)) AS VL_BC_ICMS_ST,
        CAST(s.VL_ICMS_ST AS DECIMAL(18, 2)) AS VL_ICMS_ST,
        s.COD_INF,
        CAST(s.VL_PIS AS DECIMAL(18, 2)) AS VL_PIS,
        CAST(s.VL_COFINS AS DECIMAL(18, 2)) AS VL_COFINS,
        s.TP_LIGACAO,
        s.COD_GRUPO_TENSAO,
        s.CHV_DOCE,
        s.FIN_DOCE,
        s.CHV_DOCE_REF,
        s.IND_DEST,
        s.COD_MUN_DEST,
        s.COD_CTA,
        s.COD_MOD_DOC_REF,
        s.HASH_DOC_REF,
        s.SER_DOC_REF,
        s.NUM_DOC_REF,
        s.MES_DOC_REF,
        s.ENER_INJET,
        s.OUTRAS_DED,
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
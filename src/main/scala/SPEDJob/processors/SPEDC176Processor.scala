package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC176Processor extends SPEDBaseProcessor {
  override def registro: String = "C176"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.COD_MOD_ULT_E,
        CAST(s.NUM_DOC_ULT_E AS BIGINT) AS NUM_DOC_ULT_E,
        s.SER_ULT_E,
        to_date(CAST(s.DT_ULT_E AS STRING), 'ddMMyyyy') as DT_ULT_E,
        s.COD_PART_ULT_E,
        CAST(s.QUANT_ULT_E AS DECIMAL(18,3)) AS QUANT_ULT_E,
        CAST(s.VL_UNIT_ULT_E AS DECIMAL(18,3)) AS VL_UNIT_ULT_E,
        CAST(s.VL_UNIT_BC_ST AS DECIMAL(18,3)) AS VL_UNIT_BC_ST,
        CAST(s.CHAVE_NFE_ULT_E AS STRING) AS CHAVE_NFE_ULT_E,
        CAST(s.NUM_ITEM_ULT_E AS BIGINT) AS NUM_ITEM_ULT_E,
        CAST(s.VL_UNIT_BC_ICMS_ULT_E AS DECIMAL(18,2)) AS VL_UNIT_BC_ICMS_ULT_E,
        CAST(s.ALIQ_ICMS_ULT_E AS DECIMAL(18,2)) AS ALIQ_ICMS_ULT_E,
        CAST(s.VL_UNIT_LIMITE_BC_ICMS_ULT_E AS DECIMAL(18,2)) AS VL_UNIT_LIMITE_BC_ICMS_ULT_E,
        CAST(s.VL_UNIT_ICMS_ULT_E AS DECIMAL(18,3)) AS VL_UNIT_ICMS_ULT_E,
        CAST(s.ALIQ_ST_ULT_E AS DECIMAL(18,2)) AS ALIQ_ST_ULT_E,
        CAST(s.VL_UNIT_RES AS DECIMAL(18,3)) AS VL_UNIT_RES,
        CAST(s.COD_RESP_RET AS INT) AS COD_RESP_RET,
        CAST(s.COD_MOT_RES AS INT) AS COD_MOT_RES,
        CAST(s.CHAVE_NFE_RET AS STRING) AS CHAVE_NFE_RET,
        s.COD_PART_NFE_RET,
        s.SER_NFE_RET,
        CAST(s.NUM_NFE_RET AS BIGINT) AS NUM_NFE_RET,
        CAST(s.ITEM_NFE_RET AS BIGINT) AS ITEM_NFE_RET,
        s.COD_DA,
        s.NUM_DA,
        CAST(s.VL_UNIT_RES_FCP AS DECIMAL(18,3)) AS VL_UNIT_RES_FCP,
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
package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDE210Processor extends SPEDBaseProcessor {
  override def registro: String = "E210"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        CAST(s.IND_MOV_ST AS STRING) AS IND_MOV_ST,
        CAST(s.VL_SLD_CRED_ANT_ST AS DECIMAL(18, 2)) AS VL_SLD_CRED_ANT_ST,
        CAST(s.VL_DEVOL_ST AS DECIMAL(18, 2)) AS VL_DEVOL_ST,
        CAST(s.VL_RESSARC_ST AS DECIMAL(18, 2)) AS VL_RESSARC_ST,
        CAST(s.VL_OUT_CRED_ST AS DECIMAL(18, 2)) AS VL_OUT_CRED_ST,
        CAST(s.VL_AJ_CREDITOS_ST AS DECIMAL(18, 2)) AS VL_AJ_CREDITOS_ST,
        CAST(s.VL_RETENCAO_ST AS DECIMAL(18, 2)) AS VL_RETENCAO_ST,
        CAST(s.VL_OUT_DEB_ST AS DECIMAL(18, 2)) AS VL_OUT_DEB_ST,
        CAST(s.VL_AJ_DEBITOS_ST AS DECIMAL(18, 2)) AS VL_AJ_DEBITOS_ST,
        CAST(s.VL_SLD_DEV_ANT_ST AS DECIMAL(18, 2)) AS VL_SLD_DEV_ANT_ST,
        CAST(s.VL_DEDUCOES_ST AS DECIMAL(18, 2)) AS VL_DEDUCOES_ST,
        CAST(s.VL_ICMS_RECOL_ST AS DECIMAL(18, 2)) AS VL_ICMS_RECOL_ST,
        CAST(s.VL_SLD_CRED_ST_TRANSPORTAR AS DECIMAL(18, 2)) AS VL_SLD_CRED_ST_TRANSPORTAR,
        CAST(s.DEB_ESP_ST AS DECIMAL(18, 2)) AS DEB_ESP_ST,
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
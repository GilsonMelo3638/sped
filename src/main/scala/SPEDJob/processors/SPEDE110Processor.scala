package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDE110Processor extends SPEDBaseProcessor {
  override def registro: String = "E110"
  val config: SPEDConfig = super.config(
    """
        SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          CAST(s.VL_TOT_DEBITOS AS DECIMAL(18, 2)) AS VL_TOT_DEBITOS,
          CAST(s.VL_AJ_DEBITOS AS DECIMAL(18, 2)) AS VL_AJ_DEBITOS,
          CAST(s.VL_TOT_AJ_DEBITOS AS DECIMAL(18, 2)) AS VL_TOT_AJ_DEBITOS,
          CAST(s.VL_ESTORNOS_CRED AS DECIMAL(18, 2)) AS VL_ESTORNOS_CRED,
          CAST(s.VL_TOT_CREDITOS AS DECIMAL(18, 2)) AS VL_TOT_CREDITOS,
          CAST(s.VL_AJ_CREDITOS AS DECIMAL(18, 2)) AS VL_AJ_CREDITOS,
          CAST(s.VL_TOT_AJ_CREDITOS AS DECIMAL(18, 2)) AS VL_TOT_AJ_CREDITOS,
          CAST(s.VL_ESTORNOS_DEB AS DECIMAL(18, 2)) AS VL_ESTORNOS_DEB,
          CAST(s.VL_SLD_CREDOR_ANT AS DECIMAL(18, 2)) AS VL_SLD_CREDOR_ANT,
          CAST(s.VL_SLD_APURADO AS DECIMAL(18, 2)) AS VL_SLD_APURADO,
          CAST(s.VL_TOT_DED AS DECIMAL(18, 2)) AS VL_TOT_DED,
          CAST(s.VL_ICMS_RECOLHER AS DECIMAL(18, 2)) AS VL_ICMS_RECOLHER,
          CAST(s.VL_SLD_CREDOR_TRANSPORTAR AS DECIMAL(18, 2)) AS VL_SLD_CREDOR_TRANSPORTAR,
          CAST(s.DEB_ESP AS DECIMAL(18, 2)) AS DEB_ESP,
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
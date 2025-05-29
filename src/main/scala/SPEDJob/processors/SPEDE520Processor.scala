package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDE520Processor extends SPEDBaseProcessor {
  override def registro: String = "E520"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        CAST(s.VL_SD_ANT_IPI AS DECIMAL(18, 2)) AS VL_SD_ANT_IPI,
        CAST(s.VL_DEB_IPI AS DECIMAL(18, 2)) AS VL_DEB_IPI,
        CAST(s.VL_CRED_IPI AS DECIMAL(18, 2)) AS VL_CRED_IPI,
        CAST(s.VL_OD_IPI AS DECIMAL(18, 2)) AS VL_OD_IPI,
        CAST(s.VL_OC_IPI AS DECIMAL(18, 2)) AS VL_OC_IPI,
        CAST(s.VL_SC_IPI AS DECIMAL(18, 2)) AS VL_SC_IPI,
        CAST(s.VL_SD_IPI AS DECIMAL(18, 2)) AS VL_SD_IPI,
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
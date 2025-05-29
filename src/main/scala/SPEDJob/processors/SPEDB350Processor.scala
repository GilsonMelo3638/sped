package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDB350Processor extends SPEDBaseProcessor {
  override def registro: String = "B350"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.COD_CTD,
        s.CTA_ISS,
        s.CTA_COSIF,
        s.COD_SERV,
        CAST(s.VL_CONT AS DECIMAL(18, 2)) AS VL_CONT,
        CAST(s.VL_BC_ISS AS DECIMAL(18, 2)) AS VL_BC_ISS,
        CAST(s.ALIQ_ISS AS DECIMAL(4, 2)) AS ALIQ_ISS,
        CAST(s.VL_ISS AS DECIMAL(18, 2)) AS VL_ISS,
        s.COD_INF_OBS,
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
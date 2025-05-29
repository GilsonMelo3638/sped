package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDD530Processor extends SPEDBaseProcessor {
  override def registro: String = "D530"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.IND_SERV,
        to_date(CAST(s.DT_INI_SERV AS STRING), 'ddMMyyyy') as DT_INI_SERV,
        to_date(CAST(s.DT_FIM_SERV AS STRING), 'ddMMyyyy') as DT_FIM_SERV,
        s.PER_FISCAL,
        s.COD_AREA,
        s.TERMINAL,
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
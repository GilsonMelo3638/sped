package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDD695Processor extends SPEDBaseProcessor {
  override def registro: String = "D695"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.COD_MOD,
        s.SER,
        s.NRO_ORD_INI,
        s.NRO_ORD_FIN,
        to_date(CAST(s.DT_DOC_INI AS STRING), 'ddMMyyyy') as DT_DOC_INI,
        to_date(CAST(s.DT_DOC_FIN AS STRING), 'ddMMyyyy') as DT_DOC_FIN,
        s.NOM_MEST,
        s.CHV_COD_DIG,
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
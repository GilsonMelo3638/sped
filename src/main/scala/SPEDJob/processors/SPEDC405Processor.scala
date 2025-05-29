package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC405Processor extends SPEDBaseProcessor {
  override def registro: String = "C405"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
        s.CRO,
        s.CRZ,
        s.NUM_COO_FIN,
        s.GT_FIN,
        CAST(s.VL_BRT AS DECIMAL(18, 2)) AS VL_BRT,
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
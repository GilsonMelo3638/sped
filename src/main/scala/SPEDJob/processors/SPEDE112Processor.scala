package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDE112Processor extends SPEDBaseProcessor {
  override def registro: String = "E112"
  val config: SPEDConfig = super.config(
    """
        SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          s.NUM_DA,
          s.NUM_PROC,
          s.IND_PROC,
          s.PROC,
          s.TXT_COMPL,
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
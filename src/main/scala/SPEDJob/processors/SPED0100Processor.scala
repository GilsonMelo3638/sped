package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPED0100Processor extends SPEDBaseProcessor {
  override def registro: String = "0100"

  val config: SPEDConfig = super.config(
  """
      SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          s.NOME,
          s.CPF,
          s.CRC,
          s.CNPJ,
          s.CEP,
          s.END,
          s.NUM,
          s.COMPL,
          s.BAIRRO,
          s.FONE,
          s.FAX,
          s.EMAIL,
          s.COD_MUN,
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
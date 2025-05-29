package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC197Processor extends SPEDBaseProcessor {
  override def registro: String = "C197"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.COD_AJ,
        s.DESCR_COMPL_AJ,
        s.COD_ITEM,
        CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
        CAST(s.ALIQ_ICMS AS DECIMAL(6,2)) AS ALIQ_ICMS,
        CAST(s.	VL_ICMS AS DECIMAL(18, 2)) AS 	VL_ICMS,
        CAST(s.VL_OUTROS AS DECIMAL(18, 2)) AS VL_OUTROS,
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
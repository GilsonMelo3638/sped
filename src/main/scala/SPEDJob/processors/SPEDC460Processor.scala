package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDC460Processor extends SPEDBaseProcessor {
  override def registro: String = "C460"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.COD_MOD,
        s.COD_SIT,
        s.NUM_DOC,
        to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
        CAST(s.VL_DOC AS DECIMAL(18, 2)) AS VL_DOC,
        CAST(s.VL_PIS AS DECIMAL(18, 2)) AS VL_PIS,
        CAST(s.VL_COFINS AS DECIMAL(18, 2)) AS VL_COFINS,
        s.CPF_CNPJ,
        s.NOM_ADQ,
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
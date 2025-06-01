package SPEDJob.processors

import SPEDJob.core.processor.SPEDBaseProcessor
import SPEDJob.models.SPEDConfig

object SPEDB470Processor extends SPEDBaseProcessor {
  override def registro: String = "B470"
  val config: SPEDConfig = super.config(
    """
      SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        CAST(s.VL_CONT AS DECIMAL(18, 2)) AS VL_CONT,
        CAST(s.VL_MAT_TERC AS DECIMAL(18, 2)) AS VL_MAT_TERC,
        CAST(s.VL_MAT_PROP AS DECIMAL(18, 2)) AS VL_MAT_PROP,
        CAST(s.VL_SUB AS DECIMAL(18, 2)) AS VL_SUB,
        CAST(s.VL_ISNT AS DECIMAL(18, 2)) AS VL_ISNT,
        CAST(s.VL_DED_BC AS DECIMAL(18, 2)) AS VL_DED_BC,
        CAST(s.VL_BC_ISS AS DECIMAL(18, 2)) AS VL_BC_ISS,
        CAST(s.VL_BC_ISS_RT AS DECIMAL(18, 2)) AS VL_BC_ISS_RT,
        CAST(s.VL_ISS AS DECIMAL(18, 2)) AS VL_ISS,
        CAST(s.VL_ISS_RT AS DECIMAL(18, 2)) AS VL_ISS_RT,
        CAST(s.VL_DED AS DECIMAL(18, 2)) AS VL_DED,
        CAST(s.VL_ISS_REC AS DECIMAL(18, 2)) AS VL_ISS_REC,
        CAST(s.VL_ISS_ST AS DECIMAL(18, 2)) AS VL_ISS_ST,
        CAST(s.VL_ISS_REC_UNI AS DECIMAL(18, 2)) AS VL_ISS_REC_UNI,
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
//scp "C:\Apps\Scala\sped\target\SPEDPrata-0.0.2-SNAPSHOT.jar"  gamelo@10.69.22.71:src/main/scala/SPEDPrata-0.0.2-SNAPSHOT.jar
//hdfs dfs -put -f /export/home/gamelo/src/main/scala/SPEDPrata-0.0.2-SNAPSHOT.jar /app/sped
//hdfs dfs -ls /app/sped
//hdfs dfs -rm -skipTrash /app/sped/SPEDTest-0.0.1-SNAPSHOT.jar
//  spark-submit   --class SPEDJob.SPEDProcessor   --master yarn   --deploy-mode cluster   --num-executors 20   --executor-memory 4G   --executor-cores 2   --conf "spark.sql.parquet.writeLegacyFormat=true"  hdfs://sepladbigdata/app/sped/SPEDPrata-0.0.1-SNAPSHOT.jar
package SPEDJob

import org.apache.spark.sql.SparkSession // Importa a sessão Spark

object SPEDProcessor {
  def main(args: Array[String]): Unit = {
    // Criação da SparkSession com a configuração do master para execução remota
    val spark = SparkSession.builder()
      .appName("SPED Processor")
      .master("yarn")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.executor.instances", "20")
      .getOrCreate()

    // Leitura do arquivo Parquet que contém os registros do SPED (Registro 0000) para dentro de um DataFrame r0000
    val r0000 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_0000")

    // Leitura do arquivo Parquet com a base do SPED (contendo as informações processadas do SPED) para dentro de um DataFrame sped_base
    val sped_base = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_BASE")

    // Cria uma visão temporária (view) chamada "r0000" a partir do DataFrame r0000
    r0000.createOrReplaceTempView("r0000")

    // Cria uma visão temporária (view) chamada "sped_base" a partir do DataFrame sped_base
    sped_base.createOrReplaceTempView("sped_base")

    // Executa uma consulta SQL para realizar o join entre os registros do SPED e a base processada
    val r0000xsped_base = spark.sql(
      """
SELECT
      c.*,
      s.PERIODO,
      s.ULTIMA_EFD,
      CAST((s.DATAHORA_PROCESSAMENTO / 1000) AS TIMESTAMP) AS DATAHORA_PROCESSAMENTO,
      s.STATUS_PROCESSAMENTO,
      CONCAT(SUBSTRING(CAST(c.DT_INI AS STRING), 5, 4), SUBSTRING(CAST(c.DT_INI AS STRING), 3, 2)) AS PERIODO_0000
      -- Extrai o ano (YYYY) e o mês (MM) de DDMMYYYY e concatena como YYYYMM
      FROM sped_base s
      INNER JOIN r0000 c
      ON s.id = c.id_base
      WHERE s.status_processamento IN ('10', '12', '13', '14')
  """
    )

    // Escreve o resultado da consulta r0000xsped_base em formato Parquet, particionado pela coluna "PERIODO"
    r0000xsped_base.write.mode("overwrite").partitionBy("PERIODO").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0000")

    // Define o caminho do arquivo Parquet para o Registro 0000
    val r0000Path = "/datalake/prata/sources/dbms/ADMSPED/REG_0000"

    // Leitura do arquivo Parquet REG_0000 e cache do DataFrame para melhorar o desempenho
    val r0000Cached = spark.read.parquet(r0000Path).cache()

    // Leitura do arquivo Parquet do Registro 0150 (Cadastro de Participantes)
    val r0150 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_0150")

    // Criação de visões temporárias para facilitar consultas SQL diretamente nos DataFrames
    r0000Cached.createOrReplaceTempView("r0000")
    r0150.createOrReplaceTempView("r0150")

    val r0000xr150 = spark.sql("""
    SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.COD_PART,
        s.NOME,
        LPAD(CAST(s.COD_PAIS AS STRING), 5, '0') AS COD_PAIS,
        LPAD(CAST(s.CNPJ AS STRING), 14, '0') AS CNPJ,
        LPAD(CAST(s.CPF AS STRING), 11, '0') AS CPF,
        s.IE,
        LPAD(CAST(s.COD_MUN AS STRING), 7, '0') AS COD_MUN,
        s.SUFRAMA,
        s.END,
        s.NUM,
        s.COMPL,
        s.BAIRRO,
        c.ULTIMA_EFD,
        c.PERIODO_0000,
        c.CPF AS CPF_0000,
        c.CNPJ AS CNPJ_0000,
        c.IE AS IE_0000,
        c.PERIODO AS PERIODO_SPED_BASE
    FROM r0150 s
    INNER JOIN r0000 c ON s.id_base = c.id_base
""")
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xr150.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0150")

    // Leitura do arquivo Parquet do Registro 0200 (Cadastro de Itens)
    val r0200 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_0200")
    // Criação de visões temporárias
    r0000Cached.createOrReplaceTempView("r0000")
    r0200.createOrReplaceTempView("r0200")

    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e 0200 baseado no campo "id_base"
    val r0000xr0200 = spark.sql(
      """
SELECT
          s.ID_PAI,
          s.ID,
          s.LINHA,
          s.ID_BASE,
          s.REG,
          s.COD_ITEM,
          s.DESCR_ITEM,
          s.COD_BARRA,
          s.COD_ANT_ITEM,
          s.UNID_INV,
          CAST(s.TIPO_ITEM AS INT) AS TIPO_ITEM,
          LPAD(CAST(s.COD_NCM AS STRING), 8, '0') AS COD_NCM,
          s.EX_IPI,
          LPAD(CAST(s.COD_GEN AS STRING), 2, '0') AS COD_GEN,
          s.COD_LST,
          CAST(s.ALIQ_ICMS AS DECIMAL(4, 2)) AS ALIQ_ICMS,
          LPAD(CAST(s.CEST AS STRING), 2, '0') AS CEST,
          c.ULTIMA_EFD,
          c.PERIODO_0000,
          c.CPF AS CPF_0000,
          c.CNPJ AS CNPJ_0000,
          c.IE AS IE_0000,
          c.PERIODO AS PERIODO_SPED_BASE
      FROM r0200 s
      INNER JOIN r0000 c ON s.id_base = c.id_base
  """
    )

    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xr0200.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0200")

    val r0450 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_0450")
    r0000Cached.createOrReplaceTempView("r0000")
    r0450.createOrReplaceTempView("r0450")
    val r0000xr0450 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.TXT,
            s.COD_INF,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM r0450 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    r0000xr0450.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0450")
    val rb020 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_B020")
    r0000Cached.createOrReplaceTempView("r0000")
    rb020.createOrReplaceTempView("rb020")
    // Execução da consulta SQL entre rb020 e r0000
    val r0000xrb020 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.IND_OPER,
            s.IND_EMIT,
            s.COD_PART,
            s.COD_MOD,
            s.COD_SIT,
            s.SER,
            s.NUM_DOC,
            s.CHV_NFE,
            to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
      s.COD_MUN_SERV,
            CAST(s.VL_CONT AS DECIMAL(18, 2)) AS VL_CONT,
            CAST(s.VL_MAT_TERC AS DECIMAL(18, 2)) AS VL_MAT_TERC,
            CAST(s.VL_SUB AS DECIMAL(18, 2)) AS VL_SUB,
            CAST(s.VL_ISNT_ISS AS DECIMAL(18, 2)) AS VL_ISNT_ISS,
            CAST(s.VL_DED_BC AS DECIMAL(18, 2)) AS VL_DED_BC,
            CAST(s.VL_BC_ISS AS DECIMAL(18, 2)) AS VL_BC_ISS,
            CAST(s.VL_BC_ISS_RT AS DECIMAL(18, 2)) AS VL_BC_ISS_RT,
            CAST(s.VL_ISS_RT AS DECIMAL(18, 2)) AS VL_ISS_RT,
            CAST(s.VL_ISS AS DECIMAL(18, 2)) AS VL_ISS,
            s.COD_INF_OBS,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rb020 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    r0000xrb020.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_B020")
    val rb025 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_B025")
    r0000Cached.createOrReplaceTempView("r0000")
    rb025.createOrReplaceTempView("rb025")
    // Execução da consulta SQL entre rb025 e r0000
    val r0000xrb025 = spark.sql("""
    SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        CAST(s.ALIQ_ISS AS DECIMAL(4, 2)) AS ALIQ_ISS,
        CAST(s.VL_CONT_P AS DECIMAL(18, 2)) AS VL_CONT_P,
        CAST(s.VL_BC_ISS_P AS DECIMAL(18, 2)) AS VL_BC_ISS_P,
        CAST(s.VL_ISS_P AS DECIMAL(18, 2)) AS VL_ISS_P,
        CAST(s.VL_ISNT_ISS_P AS DECIMAL(18, 2)) AS VL_ISNT_ISS_P,
        s.COD_SERV,
        c.ULTIMA_EFD,
        c.PERIODO_0000,
        c.CPF AS CPF_0000,
        c.CNPJ AS CNPJ_0000,
        c.IE AS IE_0000,
        c.PERIODO AS PERIODO_SPED_BASE
    FROM rb025 s
    INNER JOIN r0000 c ON s.id_base = c.id_base
""")
    // Escrita particionada por PERIODO_SPED_BASE
    r0000xrb025.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_B025")
    val rb440 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_B440")
    r0000Cached.createOrReplaceTempView("r0000")
    rb440.createOrReplaceTempView("rb440")
    // Execução da consulta SQL entre rb440 e r0000
    val r0000xrb440 = spark.sql("""
    SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.IND_OPER,
        s.COD_PART,
        CAST(s.VL_CONT_RT AS DECIMAL(18, 2)) AS VL_CONT_RT,
        CAST(s.VL_BC_ISS_RT AS DECIMAL(18, 2)) AS VL_BC_ISS_RT,
        CAST(s.VL_ISS_RT AS DECIMAL(18, 2)) AS VL_ISS_RT,
        c.ULTIMA_EFD,
        c.PERIODO_0000,
        c.CPF AS CPF_0000,
        c.CNPJ AS CNPJ_0000,
        c.IE AS IE_0000,
        c.PERIODO AS PERIODO_SPED_BASE
    FROM rb440 s
    INNER JOIN r0000 c ON s.id_base = c.id_base
""")
    r0000xrb440.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_B440")
    val rB470 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_B470")
    r0000Cached.createOrReplaceTempView("r0000")
    rB470.createOrReplaceTempView("rB470")
    val r0000xrB470 = spark.sql("""
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
    FROM rB470 s
    INNER JOIN r0000 c ON s.id_base = c.id_base
""")
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrB470.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_B470")
    val rc100 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_C100")
    r0000Cached.createOrReplaceTempView("r0000")
    rc100.createOrReplaceTempView("rc100")
    val r0000xrc100 = spark.sql("""
    SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        s.IND_OPER,
        s.IND_EMIT,
        s.COD_PART,
        s.COD_MOD,
        s.COD_SIT,
        s.SER,
        s.NUM_DOC,
        s.CHV_NFE,
        to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
        to_date(CAST(s.DT_E_S AS STRING), 'ddMMyyyy') as DT_E_S,
        CAST(s.VL_DOC AS DECIMAL(18, 2)) AS VL_DOC,
        s.IND_PGTO,
        CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
        CAST(s.VL_ABAT_NT AS DECIMAL(18, 2)) AS VL_ABAT_NT,
        CAST(s.VL_MERC AS DECIMAL(18, 2)) AS VL_MERC,
        s.IND_FRT,
        CAST(s.VL_FRT AS DECIMAL(18, 2)) AS VL_FRT,
        CAST(s.VL_SEG AS DECIMAL(18, 2)) AS VL_SEG,
        CAST(s.VL_OUT_DA AS DECIMAL(18, 2)) AS VL_OUT_DA,
        CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
        CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
        CAST(s.VL_BC_ICMS_ST AS DECIMAL(18, 2)) AS VL_BC_ICMS_ST,
        CAST(s.VL_ICMS_ST AS DECIMAL(18, 2)) AS VL_ICMS_ST,
        CAST(s.VL_IPI AS DECIMAL(18, 2)) AS VL_IPI,
        CAST(s.VL_PIS AS DECIMAL(18, 2)) AS VL_PIS,
        CAST(s.VL_COFINS AS DECIMAL(18, 2)) AS VL_COFINS,
        CAST(s.VL_PIS_ST AS DECIMAL(18, 2)) AS VL_PIS_ST,
        CAST(s.VL_COFINS_ST AS DECIMAL(18, 2)) AS VL_COFINS_ST,
        c.ULTIMA_EFD,
        c.PERIODO_0000,
        c.CPF AS CPF_0000,
        c.CNPJ AS CNPJ_0000,
        c.IE AS IE_0000,
        c.PERIODO AS PERIODO_SPED_BASE,
        r.CPF AS CPF_0150,
        r.CNPJ AS CNPJ_0150
    FROM rc100 s
    INNER JOIN r0000 c ON s.id_base = c.id_base
    LEFT JOIN r0150 r ON s.id_base = r.id_base AND s.COD_PART = r.COD_PART
""")
    r0000xrc100.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_C100")
    // Leitura do Registro C170 e 0200
    val rC170 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_C170")
    // Criação das visões temporárias
    r0000Cached.createOrReplaceTempView("r0000")
    rC170.createOrReplaceTempView("rC170")
    r0200.createOrReplaceTempView("r0200")
    // Execução da consulta SQL entre rC170 e r0200
    val r0000xrc170 = spark.sql("""
    SELECT
        s.ID_PAI,
        s.ID,
        s.LINHA,
        s.ID_BASE,
        s.REG,
        LPAD(CAST(s.NUM_ITEM AS STRING), 3, '0') AS NUM_ITEM,
        s.COD_ITEM,
        s.DESCR_COMPL,
        CAST(s.QTD AS DECIMAL(18, 5)) AS QTD,
        s.UNID,
        CAST(s.VL_ITEM AS DECIMAL(18, 2)) AS VL_ITEM,
        CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
        LPAD(CAST(s.CST_ICMS AS STRING), 3, '0') AS CST_ICMS,
        LPAD(CAST(s.CFOP AS STRING), 4, '0') AS CFOP,
        s.COD_NAT,
        CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
        CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
        CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
        CAST(s.VL_BC_ICMS_ST AS DECIMAL(18, 2)) AS VL_BC_ICMS_ST,
        CAST(s.ALIQ_ST AS DECIMAL(6, 2)) AS ALIQ_ST,
        CAST(s.VL_ICMS_ST AS DECIMAL(18, 2)) AS VL_ICMS_ST,
        CAST(s.VL_BC_IPI AS DECIMAL(18, 2)) AS VL_BC_IPI,
        CAST(s.ALIQ_IPI AS DECIMAL(6, 2)) AS ALIQ_IPI,
        CAST(s.VL_IPI AS DECIMAL(18, 2)) AS VL_IPI,
        LPAD(CAST(s.CST_PIS AS STRING), 3, '0') AS CST_PIS,
        CAST(s.VL_BC_PIS AS DECIMAL(18, 2)) AS VL_BC_PIS,
        CAST(s.ALIQ_PIS_PERCENT AS DECIMAL(8, 4)) AS ALIQ_PIS_PERCENT,
        CAST(s.QUANT_BC_PIS AS DECIMAL(18, 3)) AS QUANT_BC_PIS,
        CAST(s.ALIQ_PIS_REAIS AS DECIMAL(8, 4)) AS ALIQ_PIS_REAIS,
        CAST(s.VL_PIS AS DECIMAL(18, 2)) AS VL_PIS,
        LPAD(CAST(s.CST_COFINS AS STRING), 2, '0') AS CST_COFINS,
        CAST(s.VL_BC_COFINS AS DECIMAL(18, 2)) AS VL_BC_COFINS,
        CAST(s.ALIQ_COFINS_PERCENT AS DECIMAL(8, 8)) AS ALIQ_COFINS_PERCENT,
        CAST(s.QUANT_BC_COFINS AS DECIMAL(18, 3)) AS QUANT_BC_COFINS,
        CAST(s.ALIQ_COFINS_REAIS AS DECIMAL(8, 4)) AS ALIQ_COFINS_REAIS,
        CAST(s.VL_COFINS AS DECIMAL(18, 2)) AS VL_COFINS,
        CAST(s.VL_ABAT_NT AS DECIMAL(18, 2)) AS VL_ABAT_NT,
        c.ULTIMA_EFD,
        c.PERIODO_0000,
        c.CPF AS CPF_0000,
        c.CNPJ AS CNPJ_0000,
        c.IE AS IE_0000,
        c.PERIODO AS PERIODO_SPED_BASE,
        CAST(r.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS_0200
    FROM rC170 s
    INNER JOIN r0000 c ON s.id_base = c.id_base
    LEFT JOIN r0200 r ON s.id_base = r.id_base AND s.COD_ITEM = r.COD_ITEM
""")
    // Escrita particionada por PERIODO_SPED_BASE
    r0000xrc170.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_C170")
    val rc190 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_C190")
    // Criação das visões temporárias
    r0000Cached.createOrReplaceTempView("r0000")
    rc190.createOrReplaceTempView("rc190")
    // Execução da consulta SQL entre rc190 e r0000
    val r0000xrc190 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            CAST(s.CST_ICMS AS DECIMAL(3, 0)) AS CST_ICMS,
            CAST(s.CFOP AS DECIMAL(4, 0)) AS CFOP,
            CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
            CAST(s.VL_OPR AS DECIMAL(18, 2)) AS VL_OPR,
            CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
            CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
            CAST(s.VL_BC_ICMS_ST AS DECIMAL(18, 2)) AS VL_BC_ICMS_ST,
            CAST(s.VL_ICMS_ST AS DECIMAL(18, 2)) AS VL_ICMS_ST,
            CAST(s.VL_RED_BC AS DECIMAL(18, 2)) AS VL_RED_BC,
            CAST(s.VL_IPI AS DECIMAL(18, 2)) AS VL_IPI,
            s.COD_OBS,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rc190 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita particionada por PERIODO_SPED_BASE
    r0000xrc190.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_C190")
    // Leitura do arquivo Parquet do Registro 0220 (Fatores de conversão de unidades)
    val r0220 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_0220")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0150
    r0220.createOrReplaceTempView("r0220")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e 0220 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xr0220 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.UNID_CONV,
      CAST(s.FAT_CONV AS DECIMAL(18, 6)) AS FAT_CONV,
      s.COD_BARRA,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM r0220 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xr0220.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0220")
    // Leitura do arquivo Parquet do Registro 0221 (Correlação entre códigos de itens comercializados)
    val r0221 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_0221")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0221
    r0221.createOrReplaceTempView("r0221")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e 0221 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xr0221 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.COD_ITEM_ATOMICO,
      CAST(s.QTD_CONTIDA AS DECIMAL(18, 6)) AS QTD_CONTIDA,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM r0221 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xr0221.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0221")
    // Leitura do arquivo Parquet do Registro 0460 (Tabela de Observações do Lançamento Fiscal)
    val r0460 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_0460")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0460
    r0460.createOrReplaceTempView("r0460")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e 0460 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xr0460 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.COD_OBS,
      s.TXT,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM r0460 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xr0460.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_0460")
    // Leitura do arquivo Parquet do Registro C176 (Ressarcimento de ICMS e Fundo de Combate à Pobreza (FCP) em operações com Substituição Tributária (Códigos 01 e 55))
    val rC176 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_C176")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rC176
    rC176.createOrReplaceTempView("rC176")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e C176 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrC176 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.COD_MOD_ULT_E,
            CAST(s.NUM_DOC_ULT_E AS BIGINT) AS NUM_DOC_ULT_E,
            s.SER_ULT_E,
            to_date(CAST(s.DT_ULT_E AS STRING), 'ddMMyyyy') as DT_ULT_E,
            s.COD_PART_ULT_E,
            CAST(s.QUANT_ULT_E AS DECIMAL(18,3)) AS QUANT_ULT_E,
            CAST(s.VL_UNIT_ULT_E AS DECIMAL(18,3)) AS VL_UNIT_ULT_E,
            CAST(s.VL_UNIT_BC_ST AS DECIMAL(18,3)) AS VL_UNIT_BC_ST,
            CAST(s.CHAVE_NFE_ULT_E AS STRING) AS CHAVE_NFE_ULT_E,
            CAST(s.NUM_ITEM_ULT_E AS BIGINT) AS NUM_ITEM_ULT_E,
            CAST(s.VL_UNIT_BC_ICMS_ULT_E AS DECIMAL(18,2)) AS VL_UNIT_BC_ICMS_ULT_E,
            CAST(s.ALIQ_ICMS_ULT_E AS DECIMAL(18,2)) AS ALIQ_ICMS_ULT_E,
            CAST(s.VL_UNIT_LIMITE_BC_ICMS_ULT_E AS DECIMAL(18,2)) AS VL_UNIT_LIMITE_BC_ICMS_ULT_E,
            CAST(s.VL_UNIT_ICMS_ULT_E AS DECIMAL(18,3)) AS VL_UNIT_ICMS_ULT_E,
            CAST(s.ALIQ_ST_ULT_E AS DECIMAL(18,2)) AS ALIQ_ST_ULT_E,
            CAST(s.VL_UNIT_RES AS DECIMAL(18,3)) AS VL_UNIT_RES,
            CAST(s.COD_RESP_RET AS INT) AS COD_RESP_RET,
            CAST(s.COD_MOT_RES AS INT) AS COD_MOT_RES,
            CAST(s.CHAVE_NFE_RET AS STRING) AS CHAVE_NFE_RET,
            CAST(s.NUM_NFE_RET AS BIGINT) AS NUM_NFE_RET,
            CAST(s.ITEM_NFE_RET AS BIGINT) AS ITEM_NFE_RET,
            CAST(s.VL_UNIT_RES_FCP AS DECIMAL(18,3)) AS VL_UNIT_RES_FCP,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rC176 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrC176.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_C176")
    // Leitura do arquivo Parquet do Registro C195 (TObservações do lançamento fiscal (Códigos 01, 1B, 04 e 55)
    val rC195 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_C195")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rC195
    rC195.createOrReplaceTempView("rC195")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e C195 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrC195 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.COD_OBS,
      s.TXT_COMPL,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rC195 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrC195.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_C195")
    // Leitura do arquivo Parquet do Registro C197 (Outras obrigações tributárias, ajustes e informações de valores provenientes de documento fiscal)
    val rC197 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_C197")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rC197
    rC197.createOrReplaceTempView("rC197")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e C197 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrC197 = spark.sql(
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
        FROM rC197 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrC197.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_C197")
    // Leitura do arquivo Parquet do Registro D100 (Nota Fiscal de Serviço de Transporte (código 07) e CTRC (código 08), Conhecimentos de Transporte de Cargas Avulso (código 8B), Aquaviário de Cargas (código 09), Aéreo (código 10), Ferroviário de Cargas (código 11), Multimodal de Cargas (código 26), Nota Fiscal de Transporte Ferroviário de Carga (código 27); Conhecimento de Transporte Eletrônico - CT-e (código 57) e CT-e OS (código 67) e Bilhete de Passagem Eletrônico - BP-e (código 63)
    val rD100 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_D100")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rD100
    rD100.createOrReplaceTempView("rD100")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e D100 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrD100 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.IND_OPER,
            s.IND_EMIT,
            s.COD_PART,
            s.COD_MOD,
            CAST(s.COD_SIT AS INT) AS COD_SIT,
            s.SER,
            s.SUB,
            CAST(s.NUM_DOC AS BIGINT) AS NUM_DOC,
            CAST(s.CHV_CTE AS STRING) AS CHV_CTE,
            to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
            to_date(CAST(s.DT_A_P AS STRING), 'ddMMyyyy') as DT_A_P,
            CAST(s.TP_CT_E AS INT) AS TP_CT_E,
            CAST(s.CHV_CTE_REF AS STRING) AS CHV_CTE_REF,
            CAST(s.VL_DOC AS DECIMAL(18, 2)) AS VL_DOC,
            CAST(s.VL_DESC AS DECIMAL(18, 2)) AS VL_DESC,
            s.IND_FRT,
            CAST(s.VL_SERV AS DECIMAL(18, 2)) AS VL_SERV,
            CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
            CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
            CAST(s.VL_NT AS DECIMAL(18, 2)) AS VL_NT,
            s.COD_INF,
            s.COD_CTA,
            CAST(s.COD_MUN_ORIG AS INT) AS COD_MUN_ORIG,
            CAST(s.COD_MUN_DEST AS INT) AS COD_MUN_DEST,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rD100 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrD100.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_D100")
    // Leitura do arquivo Parquet do Registro D190 (Registro analítico dos documentos (Código 07, 08, 8B, 09, 10, 11, 26, 27, 57, 63 e 67)
    val rD190 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_D190")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rD190
    rD190.createOrReplaceTempView("rD190")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e D190 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrD190 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            CAST(s.CST_ICMS AS STRING) AS CST_ICMS,
            CAST(s.CFOP AS STRING) AS CFOP,
            CAST(s.ALIQ_ICMS AS DECIMAL(6, 2)) AS ALIQ_ICMS,
            CAST(s.VL_OPR AS DECIMAL(18, 2)) AS VL_OPR,
            CAST(s.VL_BC_ICMS AS DECIMAL(18, 2)) AS VL_BC_ICMS,
            CAST(s.VL_ICMS AS DECIMAL(18, 2)) AS VL_ICMS,
            CAST(s.VL_RED_BC AS DECIMAL(18, 2)) AS VL_RED_BC,
            CAST(s.COD_OBS AS STRING) AS COD_OBS,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rD190 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrD190.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_D190")
    // Leitura do arquivo Parquet do Registro E110 (Período da Apuração do ICMS)
    val rE110 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E110")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE110
    rE110.createOrReplaceTempView("rE110")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E110 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE110 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            CAST(s.VL_TOT_DEBITOS AS DECIMAL(18, 2)) AS VL_TOT_DEBITOS,
            CAST(s.VL_AJ_DEBITOS AS DECIMAL(18, 2)) AS VL_AJ_DEBITOS,
            CAST(s.VL_TOT_AJ_DEBITOS AS DECIMAL(18, 2)) AS VL_TOT_AJ_DEBITOS,
            CAST(s.VL_ESTORNOS_CRED AS DECIMAL(18, 2)) AS VL_ESTORNOS_CRED,
            CAST(s.VL_TOT_CREDITOS AS DECIMAL(18, 2)) AS VL_TOT_CREDITOS,
            CAST(s.VL_AJ_CREDITOS AS DECIMAL(18, 2)) AS VL_AJ_CREDITOS,
            CAST(s.VL_TOT_AJ_CREDITOS AS DECIMAL(18, 2)) AS VL_TOT_AJ_CREDITOS,
            CAST(s.VL_ESTORNOS_DEB AS DECIMAL(18, 2)) AS VL_ESTORNOS_DEB,
            CAST(s.VL_SLD_CREDOR_ANT AS DECIMAL(18, 2)) AS VL_SLD_CREDOR_ANT,
            CAST(s.VL_SLD_APURADO AS DECIMAL(18, 2)) AS VL_SLD_APURADO,
            CAST(s.VL_TOT_DED AS DECIMAL(18, 2)) AS VL_TOT_DED,
            CAST(s.VL_ICMS_RECOLHER AS DECIMAL(18, 2)) AS VL_ICMS_RECOLHER,
            CAST(s.VL_SLD_CREDOR_TRANSPORTAR AS DECIMAL(18, 2)) AS VL_SLD_CREDOR_TRANSPORTAR,
            CAST(s.DEB_ESP AS DECIMAL(18, 2)) AS DEB_ESP,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE110 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)

    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE110.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E110")
    // Leitura do arquivo Parquet do Registro E111 (Ajuste / benefício / incentivo da Apuração do ICMS)
    val rE111 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E111")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE111
    rE111.createOrReplaceTempView("rE111")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E111 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE111 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
      s.COD_AJ_APUR,
      s.DESCR_COMPL_AJ,
            CAST(s.VL_AJ_APUR AS DECIMAL(18, 2)) AS VL_AJ_APUR,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE111 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)

    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE111.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E111")
    // Leitura do arquivo Parquet do Registro E112 (Informações Adicionais dos Ajustes da Apuração do ICMS)
    val rE112 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E112")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE112
    rE112.createOrReplaceTempView("rE112")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E112 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE112 = spark.sql(
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
        FROM rE112 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)

    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE112.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E112")
    // Leitura do arquivo Parquet do Registro E113 (Informações adicionais dos ajustes da apuração do ICMS - Identificação dos documentos fiscais)
    val rE113 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E113")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE113
    rE113.createOrReplaceTempView("rE113")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E113 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE113 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.COD_PART,
            s.COD_MOD,
            s.SER,
            s.SUB,
            s.NUM_DOC,
            to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
            s.COD_ITEM,
            CAST(s.VL_AJ_ITEM AS DECIMAL(18, 2)) AS VL_AJ_ITEM,
            s.CHV_DOCe,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE113 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE113.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E113")
    // Leitura do arquivo Parquet do Registro E115 (Informações adicionais da Apuração - Valores declaratórios)
    val rE115 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E115")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE115
    rE115.createOrReplaceTempView("rE115")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E115 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE115 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            CAST(s.	VL_INF_ADIC AS DECIMAL(18, 2)) AS VL_INF_ADIC,
            s.DESCR_COMPL_AJ,
            s.COD_INF_ADIC,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE115 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE115.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E115")
    // Leitura do arquivo Parquet do Registro E116 (Obrigações do ICMS Recolhido ou a Recolher - Operações Próprias)
    val rE116 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E116")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE116
    rE116.createOrReplaceTempView("rE116")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E116 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE116 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.COD_OR,
            CAST(s.VL_OR AS DECIMAL(18, 2)) AS VL_OR,
            to_date(CAST(s.DT_VCTO AS STRING), 'ddMMyyyy') as DT_VCTO,
            s.COD_REC,
            s.NUM_PROC,
            s.IND_PROC,
            s.PROC,
            s.TXT_COMPL,
            s.MES_REF,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE116 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)

    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE116.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E116")
    // Leitura do arquivo Parquet do Registro E210 (Apuração do ICMS - Substituição Tributária)
    val rE210 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E210")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE210
    rE210.createOrReplaceTempView("rE210")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E210 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE210 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            CAST(s.IND_MOV_ST AS STRING) AS IND_MOV_ST,
            CAST(s.VL_SLD_CRED_ANT_ST AS DECIMAL(18, 2)) AS VL_SLD_CRED_ANT_ST,
            CAST(s.VL_DEVOL_ST AS DECIMAL(18, 2)) AS VL_DEVOL_ST,
            CAST(s.VL_RESSARC_ST AS DECIMAL(18, 2)) AS VL_RESSARC_ST,
            CAST(s.VL_OUT_CRED_ST AS DECIMAL(18, 2)) AS VL_OUT_CRED_ST,
            CAST(s.VL_AJ_CREDITOS_ST AS DECIMAL(18, 2)) AS VL_AJ_CREDITOS_ST,
            CAST(s.VL_RETENCAO_ST AS DECIMAL(18, 2)) AS VL_RETENCAO_ST,
            CAST(s.VL_OUT_DEB_ST AS DECIMAL(18, 2)) AS VL_OUT_DEB_ST,
            CAST(s.VL_AJ_DEBITOS_ST AS DECIMAL(18, 2)) AS VL_AJ_DEBITOS_ST,
            CAST(s.VL_SLD_DEV_ANT_ST AS DECIMAL(18, 2)) AS VL_SLD_DEV_ANT_ST,
            CAST(s.VL_DEDUCOES_ST AS DECIMAL(18, 2)) AS VL_DEDUCOES_ST,
            CAST(s.VL_ICMS_RECOL_ST AS DECIMAL(18, 2)) AS VL_ICMS_RECOL_ST,
            CAST(s.VL_SLD_CRED_ST_TRANSPORTAR AS DECIMAL(18, 2)) AS VL_SLD_CRED_ST_TRANSPORTAR,
            CAST(s.DEB_ESP_ST AS DECIMAL(18, 2)) AS DEB_ESP_ST,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE210 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE210.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E210")
    // Leitura do arquivo Parquet do Registro E220 (Ajuste / Benefício / Incentivo da Apuração do ICMS Substituição Tributária)
    val rE220 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E220")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE220
    rE220.createOrReplaceTempView("rE220")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E220 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE220 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
      s.COD_AJ_APUR,
      s.DESCR_COMPL_AJ,
            CAST(s.VL_AJ_APUR AS DECIMAL(18, 2)) AS VL_AJ_APUR,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE220 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE220.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E220")
    // Leitura do arquivo Parquet do Registro E230 (Informações Adicionais dos Ajustes da Apuração do ICMS)
    val rE230 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E230")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE230
    rE230.createOrReplaceTempView("rE230")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E230 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE230 = spark.sql(
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
        FROM rE230 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)

    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE230.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E230")
    // Leitura do arquivo Parquet do Registro E240 (Informações adicionais dos ajustes da apuração do ICMS substituição tributária - Identificação dos documentos fiscais)
    val rE240 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E240")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE240
    rE240.createOrReplaceTempView("rE240")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E240 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE240 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.COD_PART,
            s.COD_MOD,
            s.SER,
            s.SUB,
            s.NUM_DOC,
            to_date(CAST(s.DT_DOC AS STRING), 'ddMMyyyy') as DT_DOC,
            s.COD_ITEM,
            CAST(s.VL_AJ_ITEM AS DECIMAL(18, 2)) AS VL_AJ_ITEM,
            s.CHV_DOCe,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE240 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE240.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E240")
    // Leitura do arquivo Parquet do Registro E250 (Obrigações do ICMS Recolhido ou a Recolher - Substituição Tributária)
    val rE250 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E250")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE250
    rE250.createOrReplaceTempView("rE250")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E250 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE250 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            s.COD_OR,
            CAST(s.VL_OR AS DECIMAL(18, 2)) AS VL_OR,
            to_date(CAST(s.DT_VCTO AS STRING), 'ddMMyyyy') as DT_VCTO,
            s.COD_REC,
            s.NUM_PROC,
            s.IND_PROC,
            s.PROC,
            s.TXT_COMPL,
            s.MES_REF,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE250 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)

    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE250.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E250")
    // Leitura do arquivo Parquet do Registro E310 (Apuração do Fundo de combate a pobreza e do ICMS Diferencial de Alíquota - UF origem/destino EC 87/15 (Válido a partir de 01/01/2017))
    val rE310 = spark.read.parquet("/datalake/bronze/sources/dbms/ADMSPED/SPED_REG_E310")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame r0000
    r0000Cached.createOrReplaceTempView("r0000")
    // Criação de uma visão temporária para facilitar consultas SQL diretamente no DataFrame rE310
    rE310.createOrReplaceTempView("rE310")
    // Execução de uma consulta SQL que faz o JOIN entre os registros 0000 e E310 baseado no campo "id_base"
    // São selecionados e formatados diversos campos, incluindo a formatação de CPF, CNPJ e outros campos para garantir consistência no número de dígitos
    val r0000xrE310 = spark.sql(
      """
        SELECT
            s.ID_PAI,
            s.ID,
            s.LINHA,
            s.ID_BASE,
            s.REG,
            CAST(s.IND_MOV_FCP_DIFAL AS STRING) AS IND_MOV_FCP_DIFAL,
            CAST(s.VL_SLD_CRED_ANT_DIFAL AS DECIMAL(18, 2)) AS VL_SLD_CRED_ANT_DIFAL,
            CAST(s.VL_TOT_DEBITOS_DIFAL AS DECIMAL(18, 2)) AS VL_TOT_DEBITOS_DIFAL,
            CAST(s.VL_OUT_DEB_DIFAL AS DECIMAL(18, 2)) AS VL_OUT_DEB_DIFAL,
            CAST(s.VL_TOT_CREDITOS_DIFAL AS DECIMAL(18, 2)) AS VL_TOT_CREDITOS_DIFAL,
            CAST(s.VL_OUT_CRED_DIFAL AS DECIMAL(18, 2)) AS VL_OUT_CRED_DIFAL,
            CAST(s.VL_SLD_DEV_ANT_DIFAL AS DECIMAL(18, 2)) AS VL_SLD_DEV_ANT_DIFAL,
            CAST(s.VL_DEDUCOES_DIFAL AS DECIMAL(18, 2)) AS VL_DEDUCOES_DIFAL,
            CAST(s.VL_RECOL_DIFAL AS DECIMAL(18, 2)) AS VL_RECOL_DIFAL,
            CAST(s.VL_SLD_CRED_TRANSPORTAR_DIFAL AS DECIMAL(18, 2)) AS VL_SLD_CRED_TRANSPORTAR_DIFAL,
            CAST(s.DEB_ESP_DIFAL AS DECIMAL(18, 2)) AS DEB_ESP_DIFAL,
            CAST(s.VL_SLD_CRED_ANT_FCP AS DECIMAL(18, 2)) AS VL_SLD_CRED_ANT_FCP,
            CAST(s.VL_TOT_DEB_FCP AS DECIMAL(18, 2)) AS VL_TOT_DEB_FCP,
            CAST(s.VL_OUT_DEB_FCP AS DECIMAL(18, 2)) AS VL_OUT_DEB_FCP,
            CAST(s.VL_TOT_CRED_FCP AS DECIMAL(18, 2)) AS VL_TOT_CRED_FCP,
            CAST(s.VL_OUT_CRED_FCP AS DECIMAL(18, 2)) AS VL_OUT_CRED_FCP,
            CAST(s.VL_SLD_DEV_ANT_FCP AS DECIMAL(18, 2)) AS VL_SLD_DEV_ANT_FCP,
            CAST(s.VL_DEDUCOES_FCP AS DECIMAL(18, 2)) AS VL_DEDUCOES_FCP,
            CAST(s.VL_RECOL_FCP AS DECIMAL(18, 2)) AS VL_RECOL_FCP,
            CAST(s.VL_SLD_CRED_TRANSPORTAR_FCP AS DECIMAL(18, 2)) AS VL_SLD_CRED_TRANSPORTAR_FCP,
            CAST(s.DEB_ESP_FC AS DECIMAL(18, 2)) AS DEB_ESP_FC,
            c.ULTIMA_EFD,
            c.PERIODO_0000,
            c.CPF AS CPF_0000,
            c.CNPJ AS CNPJ_0000,
            c.IE AS IE_0000,
            c.PERIODO AS PERIODO_SPED_BASE
        FROM rE310 s
        INNER JOIN r0000 c ON s.id_base = c.id_base
    """)
    // Escrita do resultado em formato Parquet, particionado pela coluna "PERIODO_SPED_BASE"
    r0000xrE310.write.mode("overwrite").partitionBy("PERIODO_SPED_BASE").parquet("/datalake/prata/sources/dbms/ADMSPED/REG_E310")
  }
}

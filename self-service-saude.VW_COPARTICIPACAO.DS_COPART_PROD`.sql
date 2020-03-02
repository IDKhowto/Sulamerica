CREATE OR REPLACE TABLE `self-service-saude.VW_COPARTICIPACAO.DS_COPART_PROD` AS(
select *,   
case 
  when porcen_copart in ('0%') 
    THEN 'NAO COPARTICIPADO' 
      else 'COPARTICIPADO' end as COPART_2

from
(



SELECT
  DIGITO_EMPRESA,
  COD_EMPRESA,
  COMPANHIA,
  PRODUTO,
  RAZAO_SOCIAL,
  COD_PROD,
  EMPRESA,
  FLG_COPART_REVERS,
  REVERSIVEL,
  FLG_CONTR_ADESAO,
  CONTRATO_ADESAO,
  PLANO,
  GRUPO_ESTAT_INI,
  GRUPO_ESTAT_FIM,
  COD_PLANO,
  COD_ACUMULADOR1,
  COD_UNIDADE_ACUM1,
  FLG_REEMBOLSO,
  REEMBOLSO,
  FLG_INTERNACAO,
  INTERNACAO,
  DESCRICAO_GRUPO,
  GRUPO_ESTATISTICO,
  TIPO_GRUPO,
  DESCRICAO_PRODUTO,
  COD_TIPO_PRODUTO,
  CARTEIRA,
  DATAMAXIN,
  DAT_INIC_VIG_ESP,
  DAT_FIM_VIG_ESP,
  DAT_INI_VIG_CAT,
  DAT_FIM_VIG_CAT,
  DT_INIC_ACUM,
  case  
    when VALOR_PARTICIPACAO in ('0.00') 
      then ('0')
    when VALOR_PARTICIPACAO IN ('0')
      THEN ('0') ELSE VALOR_PARTICIPACAO
      END AS VALOR_PARTICIPACAO,
  
  concat(VALOR_PARTICIPACAO, "%") AS PORCEN_COPART,

  UNIDADE_PARTICIPACAO,
  VALOR_LIMITE,
  UNIDADE_LIMITE,
  LAG_VAL_LIMITE_PARTIC,
  LAG_VAL_PARTIC_MAX,
  VALOR_MAXIMO,
  UNIDADE_MAXIMA,
  ROTINA_CALCULO_ACUM,
  VALOR_PARTIC_FUNC,
  COD_ESPECIFICACAO,
  GRUPO_ESTATC,
  COD_ROT_PESQ_UNID,
  COPARTICIPACAO,
  COD_ACUMULADOR2,
  VAL_LIMITE_ACUM2,
  COD_UNIDADE_ACUM2,
  COD_ROT_CALC_ACUM2,
  COD_ACUMULADOR3,
  COD_UNIDADE_ACUM3,
  COD_ROT_CALC_ACUM3,
  COD_ACUMULADOR4,
  VAL_LIMITE_ACUM4,
  COD_UNIDADE_ACUM4,
  COD_ROT_CALC_ACUM4,
  DAT_MANUTENCAO,
  COD_CIA_RESP,
  COD_REG_RESP,
  VAL_PCT_COB_COPART,
  NME_TRANSACAO_CDC,
  IDC_TIPO_TRANSACAO_CDC,
  TMP_COMMIT_ORIGEM_CDC,
  TMP_SINCRO_DESTINO_CDC
FROM
  `self-service-saude.VW_COPARTICIPACAO.TB_COPART_PROVISORIA`
  )
)

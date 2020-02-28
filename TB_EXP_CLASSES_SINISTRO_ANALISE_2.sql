TB_EXP_CLASSES_SINISTRO_ANALISE_2

CREATE OR REPLACE TABLE `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_ANALISE` AS 

WITH CTE_SIN_1 AS(
SELECT T1.COD_BENEFICIARIO_CHAVE 
      ,T1.VAL_SINISTRO
      ,T1.FLG_TIPO_SINISTRO 
      ,T2.DSC_GRUPO_SERVICO
      ,CONCAT(CAST(EXTRACT(YEAR FROM T1.DAT_EXECUCAO) AS STRING),CAST(EXTRACT(MONTH FROM T1.DAT_EXECUCAO) AS STRING)) AS ANO_MES
  FROM `self-service-saude.VW_CONS_SAU.DM_MGR_SINISTRO` T1
  LEFT JOIN  `self-service-saude.VW_CONS_SAU.TB_DIM_SERVICO` T2
    ON T1.COD_SERVICO = T2.COD_SERVICO 
--  WHERE T1.DAT_EXECUCAO >= '2019-09-01'
--    --AND T1.DAT_EXECUCAO <= '2019-06-30'
--    --AND T1.DAT_EXECUCAO <= DATE_TRUNC(DATE_SUB(CURRENT_DATE,INTERVAL 1 MONTH),MONTH)
--   AND T1.COD_BENEFICIARIO_CHAVE = '0000700021215011'
)--SELECT * FROM CTE_SIN_1
,CTE_SIN_2 AS (
SELECT T1.COD_BENEFICIARIO_CHAVE
      ,T1.VAL_SINISTRO
      ,T1.FLG_TIPO_SINISTRO     
      ,T1.DSC_GRUPO_SERVICO
      ,CAST(CASE WHEN ANO_MES IN ('20171','20172','20173')
            THEN '1'
            WHEN ANO_MES IN ('20174','20175','20176')
            THEN '2'
            WHEN ANO_MES IN ('20177','20178','20179')
            THEN '3'
            WHEN ANO_MES IN ('201710','201711','201712')
            THEN '4'
            WHEN ANO_MES IN ('20181','20182','20183')
            THEN '5'
            WHEN ANO_MES IN ('20184','20185','20186')
            THEN '6'
            WHEN ANO_MES IN ('20187','20188','20189')
            THEN '7'
            WHEN ANO_MES IN ('201810','201811','201812')
            THEN '8'
            WHEN ANO_MES IN ('20191','20192','20193')
            THEN '9'
            WHEN ANO_MES IN ('20194','20195','20196')
            THEN '10'
            WHEN ANO_MES IN ('20197','20198','20199')
            THEN '11'
            WHEN ANO_MES IN ('201910','201911','201912')
            THEN '12'
            ELSE '0' 
         END AS NUMERIC) AS COD_TRIMESTRE
  FROM CTE_SIN_1 T1      
 )
,CTE_SIN_3 AS (
SELECT T1.COD_BENEFICIARIO
      ,T1.TRI
      ,T1.COD_APRESENTACAO_EMPRESA
      ,T1.COD_BENEFICIARIO_CHAVE
      ,T1.IDC_TIPO_PLANO
      ,T1.DSC_SEXO
      ,T1.DSC_CARTEIRA
      ,T1.DAT_NASCIMENTO
      ,T1.DSC_SITUACAO_TITULAR
      ,T1.DSC_EST_CIVIL
      ,T1.QTD_ATIVO
      ,CAST(T1.VAL_CLS AS NUMERIC) AS VAL_CLS
      ,CAST(T1.VAL_AMB AS NUMERIC) AS VAL_AMB
      ,CAST(T1.VAL_INTER AS NUMERIC) AS VAL_INTER
      ,T1.QTD_INTER
      ,T1.CEM_17_ANO
      ,T1.CEM_18_ANO
      ,T1.CEM_19_ANO
      ,T1.CEM_TRI
      ,T1.COORTE1
      ,T1.COORTE2
      ,T1.COORTE3
      ,T1.COORTE4
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'CONSULTA'        ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_CONSULTA_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'DIAGNOSE'        ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_DIAGNOSE_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'DIARIAS'         ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_DIARIAS_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'GASOTERAPIA'     ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_GASOTERAPIA_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'MATERIAL'        ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_MATERIAL_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'MEDICAMENTO'     ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_MEDICAMENTO_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'PACOTE'          ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_PACOTE_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'PROCEDIMENTO'    ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_PROCEDIMENTO_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'TAXA'            ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_TAXA_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'TAXA DE SALA'    ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_TAXA_SALA_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'TERAPIA'         ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_TERAPIA_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) = 'JUDICIAL'        ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_JUDICIAL_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'A' AND TRIM(DSC_GRUPO_SERVICO) NOT IN ('CONSULTA'
                                                                      ,'DIAGNOSE'
                                                                      ,'DIARIAS'
                                                                      ,'GASOTERAPIA'
                                                                      ,'MATERIAL'
                                                                      ,'MEDICAMENTO'
                                                                      ,'PACOTE'
                                                                      ,'PROCEDIMENTO'
                                                                      ,'TAXA'
                                                                      ,'TAXA DE SALA'
                                                                      ,'TERAPIA'
                                                                      ,'JUDICIAL'),ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_NAO_CLASSIFICADO_AMBULATORIO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'CONSULTA'        ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_CONSULTA_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'DIAGNOSE'        ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_DIAGNOSE_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'DIARIAS'         ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_DIARIAS_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'GASOTERAPIA'     ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_GASOTERAPIA_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'MATERIAL'        ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_MATERIAL_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'MEDICAMENTO'     ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_MEDICAMENTO_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'PACOTE'          ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_PACOTE_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'PROCEDIMENTO'    ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_PROCEDIMENTO_INTERNACAO

      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'TAXA'            ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_TAXA_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'TAXA DE SALA'    ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_TAXA_SALA_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'TERAPIA'         ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_TERAPIA_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) = 'JUDICIAL'        ,ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_JUDICIAL_INTERNACAO
      ,IF(FLG_TIPO_SINISTRO = 'I' AND TRIM(DSC_GRUPO_SERVICO) NOT IN ('CONSULTA'
                                                                      ,'DIAGNOSE'
                                                                      ,'DIARIAS'
                                                                      ,'GASOTERAPIA'
                                                                      ,'MATERIAL'
                                                                      ,'MEDICAMENTO'
                                                                      ,'PACOTE'
                                                                      ,'PROCEDIMENTO'
                                                                      ,'TAXA'
                                                                      ,'TAXA DE SALA'
                                                                      ,'TERAPIA'
                                                                      ,'JUDICIAL'),ROUND(SUM(VAL_SINISTRO),2),0) AS VAL_SINISTRO_NAO_CLASSIFICADO_INTERNACAO     
  FROM `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_TOP` T1
  LEFT JOIN CTE_SIN_2 T2
    ON T1.COD_BENEFICIARIO_CHAVE = T2.COD_BENEFICIARIO_CHAVE
   AND T1.TRI                    = T2.COD_TRIMESTRE
--    WHERE T1.COD_BENEFICIARIO_CHAVE = '0000700021215011' 
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,FLG_TIPO_SINISTRO,DSC_GRUPO_SERVICO
--  ORDER BY TRI
)
SELECT COD_BENEFICIARIO
      ,TRI
      ,COD_APRESENTACAO_EMPRESA
      ,COD_BENEFICIARIO_CHAVE
      ,IDC_TIPO_PLANO
      ,DSC_SEXO
      ,DSC_CARTEIRA
      ,DAT_NASCIMENTO
      ,DSC_SITUACAO_TITULAR
      ,DSC_EST_CIVIL
      ,QTD_ATIVO
      ,VAL_CLS
      ,VAL_AMB
      ,VAL_INTER
      ,QTD_INTER
      ,CEM_17_ANO
      ,CEM_18_ANO
      ,CEM_19_ANO
      ,CEM_TRI
      ,COORTE1
      ,COORTE2
      ,COORTE3
      ,COORTE4
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_CONSULTA_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_CONSULTA_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_DIAGNOSE_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_DIAGNOSE_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_DIARIAS_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_DIARIAS_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_GASOTERAPIA_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_GASOTERAPIA_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_MATERIAL_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_MATERIAL_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_MEDICAMENTO_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_MEDICAMENTO_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_PACOTE_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_PACOTE_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_PROCEDIMENTO_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_PROCEDIMENTO_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_TAXA_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_TAXA_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_TAXA_SALA_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_TAXA_SALA_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_TERAPIA_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_TERAPIA_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_JUDICIAL_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_JUDICIAL_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_NAO_CLASSIFICADO_AMBULATORIO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_NAO_CLASSIFICADO_AMBULATORIO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_CONSULTA_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_CONSULTA_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_DIAGNOSE_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_DIAGNOSE_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_DIARIAS_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_DIARIAS_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_GASOTERAPIA_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_GASOTERAPIA_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_MATERIAL_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_MATERIAL_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_MEDICAMENTO_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_MEDICAMENTO_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_PACOTE_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_PACOTE_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_PROCEDIMENTO_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_PROCEDIMENTO_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_TAXA_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_TAXA_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_TAXA_SALA_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_TAXA_SALA_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_TERAPIA_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_TERAPIA_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_JUDICIAL_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_JUDICIAL_INTERNACAO
      ,IF(VAL_CLS > 0 ,ROUND(SUM(VAL_SINISTRO_NAO_CLASSIFICADO_INTERNACAO),2)/VAL_CLS,0) AS PERCENT_SINISTRO_NAO_CLASSIFICADO_INTERNACAO
  FROM CTE_SIN_3
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 
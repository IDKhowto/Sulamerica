CREATE OR REPLACE TABLE `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_TOP_ABERTO_5000` As

WITH CTE_TIPINTER AS (
SELECT CAST(COD_TIPO_INTERN AS NUMERIC) AS COD_TIPO_INTERN
      ,UPPER(DSC_TIPO_INTERN) AS DSC_TIPO_INTERN
      ,ROW_NUMBER() OVER(PARTITION BY COD_TIPO_INTERN ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) AS RNK
  FROM `self-service-saude.OW_LAND_SAU_PRD.TISS_TIPO_INTERNACAO` 
)
,CTE_TIPINTER_2 AS (
SELECT COD_TIPO_INTERN
      ,DSC_TIPO_INTERN
  FROM CTE_TIPINTER
 WHERE RNK = 1
)
--SELECT * FROM CTE_TIPINTER 
,CTE_VIDA AS(
SELECT DISTINCT 
       T1.COD_BENEFICIARIO_CHAVE
      ,T1.COD_CARTEIRINHA_ORIGINAL 
  FROM `self-service-saude.VW_CONS_SAU.DM_MGR_VIDA` T1
 INNER JOIN `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_BATMAN` T2
    ON T1.COD_BENEFICIARIO_CHAVE = T2.COD_BENEFICIARIO_CHAVE 
)   
,CTE_SIN_1 AS(
SELECT 
T1.COD_BENEFICIARIO_CHAVE 
      ,T1.NME_FANTASIA_PRESTADOR 
      ,T1.VAL_SINISTRO
      ,T1.FLG_TIPO_SINISTRO
      ,T1.COD_PRESTADOR
      ,T1.FLG_LIMINAR
      ,CAST(T1.COD_TPO_ATEND AS NUMERIC) AS COD_TPO_ATEND
      ,T1.DAT_EXECUCAO
      ,T1.DAT_ATENDIMENTO
      ,T7.DAT_INTERNACAO
      ,T1.COD_SERVICO
      ,T2.DSC_SERVICO_ABREV 
      ,T2.DSC_SERVICO_COMPL AS DSC_SERVICO 
      ,T2.DSC_GRUPO_SERVICO
      ,T2.DSC_SUB_GRUPO_SERV AS DSC_SUBGRUPO 
      ,T1.QTD_SERVICO
      ,T3.NME_GRP_ECON_PREST
      ,T7.DAT_ALTA 
      ,T3.NME_MUNICIPIO 
      ,T3.SIG_UF 
      ,T4.DSC_ESPECIALIDADE 
--       ,T5.DSC_TIPO_INTERN
      ,T6.DSC_TIPO_ACOMODACAO
      ,T7.IDT_DOCUMENTO_AP AS NUM_VPP
      ,T7.FLG_REINTERNACAO
      ,T1.DSC_TIPO_SINISTRO 				
     ,CONCAT(CAST(EXTRACT(YEAR FROM T1.DAT_EXECUCAO) AS STRING),CAST(EXTRACT(MONTH FROM T1.DAT_EXECUCAO) AS STRING)) AS ANO_MES
  FROM `self-service-saude.VW_CONS_SAU.DM_MGR_SINISTRO` T1
   INNER JOIN  `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_BATMAN` BAT
     ON T1.COD_BENEFICIARIO_CHAVE = BAT.COD_BENEFICIARIO_CHAVE
  LEFT JOIN  `self-service-saude.VW_CONS_SAU.TB_DIM_SERVICO` T2
    ON T1.COD_SERVICO = T2.COD_SERVICO 
  LEFT JOIN  `self-service-saude.VW_CONS_SAU.TB_DIM_PRESTADOR` T3
    ON T1.COD_PRESTADOR = T3.COD_PRESTADOR 
  LEFT JOIN `self-service-saude.SANDBOX_DITER.TB_DIM_CLASSE_MEDICA_DITER` T4
    ON T1.COD_SERVICO = T4.COD_SERVICO 
--   LEFT JOIN CTE_TIPINTER T5
--     ON CAST(T1.COD_TPO_ATEND AS NUMERIC) = T5.COD_TIPO_INTERN 
  LEFT OUTER JOIN `self-service-saude.VW_CONS_SAU.VW_DIM_CATEGORIA_EMPRESA` T6
    ON T1.COD_EMPRESA = T6.COD_EMPRESA
   AND T1.COD_PREF_EMPRESA = T6.COD_PREF_EMPRESA
   AND T1.COD_CATEGORIA = T6.COD_CATEGORIA    
   
   LEFT JOIN `self-service-saude.VW_CONS_SAU.DM_MGR_INTERNACAO_DITER` T7
     ON T1.COD_BENEFICIARIO_CHAVE   = T7.COD_BENEFICIARIO_CHAVE 
    --AND T1.DAT_PAGAMENTO            = T7.DAT_INICIAL 
    AND T1.IDT_DOCUMENTO_AP         = T7.IDT_DOCUMENTO_AP 

 WHERE T1.DAT_EXECUCAO >= '2017-01-01'
--    AND T5.RNK = 1
   AND T1.DAT_EXECUCAO <= DATE_TRUNC(DATE_SUB(CURRENT_DATE,INTERVAL 1 MONTH),MONTH)
--    AND T1.COD_BENEFICIARIO_CHAVE = '7865700006400010'
)--SELECT DISTINCT DSC_TIPO_INTERN FROM CTE_SIN_1
,CTE_SIN_2 AS (
SELECT T1.COD_BENEFICIARIO_CHAVE
      ,T1.NME_FANTASIA_PRESTADOR
      ,T1.VAL_SINISTRO
      ,T1.FLG_TIPO_SINISTRO
      ,T1.COD_PRESTADOR
      ,T1.DSC_ESPECIALIDADE
      ,T1.DSC_SERVICO_ABREV
      ,T1.DSC_SERVICO 
      ,T1.DSC_GRUPO_SERVICO
      ,T1.DSC_SUBGRUPO
      ,T1.QTD_SERVICO
      ,T1.NME_GRP_ECON_PREST 
      ,T1.NME_MUNICIPIO 
      ,T1.SIG_UF
      ,T1.COD_TPO_ATEND
--       ,T1.DSC_TIPO_INTERN
      ,T1.DSC_TIPO_ACOMODACAO
      ,T1.NUM_VPP
      ,T1.FLG_REINTERNACAO
      ,T1.DSC_TIPO_SINISTRO 
      ,T1.DAT_EXECUCAO
      ,T1.COD_SERVICO
      ,T1.DAT_INTERNACAO
      ,T1.DAT_ALTA
      ,T1.DAT_ATENDIMENTO
      ,CASE WHEN UPPER(TRIM(T1.DSC_GRUPO_SERVICO)) = 'JUDICIAL'
             AND T1.FLG_LIMINAR = 'S'
            THEN 1
            ELSE 0
        END AS FLG_LIMINAR
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

,CTE_2 AS(
SELECT T1.*
      ,VIDA.COD_CARTEIRINHA_ORIGINAL 
      ,T2.NME_FANTASIA_PRESTADOR
      ,T2.FLG_TIPO_SINISTRO
      ,T2.COD_PRESTADOR
      ,T2.DSC_ESPECIALIDADE
     ,T2.DAT_ATENDIMENTO
     ,T2.DAT_ALTA
      ,T2.DSC_SERVICO_ABREV
      ,T2.DSC_SERVICO 
      ,T2.DSC_GRUPO_SERVICO
      ,T2.DSC_SUBGRUPO
      ,T2.NME_GRP_ECON_PREST 
      ,T2.NME_MUNICIPIO 
      ,T2.SIG_UF      
      ,T3.DSC_TIPO_INTERN
      ,T2.FLG_LIMINAR
      ,T2.DSC_TIPO_ACOMODACAO
      ,T2.NUM_VPP
      ,T2.FLG_REINTERNACAO
      ,T2.DSC_TIPO_SINISTRO 
      ,T2.DAT_EXECUCAO
      ,T2.COD_SERVICO
      ,ROUND(CAST(SUM(VAL_SINISTRO) AS NUMERIC),2) AS VAL_SINISTRO
      ,SUM(T2.QTD_SERVICO) AS QTD_SERVICO
  FROM `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_TOP` T1
   INNER JOIN  `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_BATMAN` BAT
     ON T1.COD_BENEFICIARIO_CHAVE = BAT.COD_BENEFICIARIO_CHAVE 
  INNER JOIN  CTE_VIDA VIDA
     ON T1.COD_BENEFICIARIO_CHAVE = VIDA.COD_BENEFICIARIO_CHAVE 
  LEFT JOIN CTE_SIN_2 T2
    ON T1.COD_BENEFICIARIO_CHAVE = T2.COD_BENEFICIARIO_CHAVE
   AND T1.TRI                    = T2.COD_TRIMESTRE
  LEFT JOIN CTE_TIPINTER_2 T3
    ON T2.COD_TPO_ATEND = T3.COD_TIPO_INTERN   
--  WHERE T1.COD_BENEFICIARIO_CHAVE = '7865700006400010'   
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45
 --ORDER BY TRI
)


select * from cte_2
-- where COD_BENEFICIARIO_CHAVE in ('0900101812910001') and cte_2.NUM_vpp in ('1801086215')
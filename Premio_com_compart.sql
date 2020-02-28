-- PREMIO POR PLANO

SELECT
A.ANO_MES_EMISSAO
,A.COD_PLANO 
, E.COD_PROD
,E.FLG_COPART_REVERS
, PS.COD_TIPO_PRODUTO
, (case WHEN PS.COD_TIPO_PRODUTO = 'A' THEN 'ADMINISTRADO'
				WHEN PS.COD_TIPO_PRODUTO = 'P' THEN 'PME'
				WHEN PS.COD_TIPO_PRODUTO = 'G' AND (E.FLG_CONTR_ADESAO = 'N' OR E.FLG_CONTR_ADESAO IS NULL) and E.COD_PROD not IN (567,563) THEN 'EMPRESARIAL'
				WHEN PS.COD_TIPO_PRODUTO = 'G' AND E.FLG_CONTR_ADESAO = 'S' THEN 'ADESAO'
        WHEN PS.COD_TIPO_PRODUTO = 'G' AND E.COD_PROD IN (567,563) AND (E.FLG_CONTR_ADESAO = 'N' OR E.FLG_CONTR_ADESAO IS NULL) THEN 'PME+'              
				WHEN PS.COD_TIPO_PRODUTO = 'I' THEN 'INDIVIDUAL'
				ELSE 'NÃO IDENTIFICADO' END) AS CARTEIRA       
, CE.NME_CATEGORIA 
, CA.IDC_TIPO_PLANO 
,case when PM.COPART is null then 'N' ELSE PM.COPART END AS COPART    
,FORMAT("%.2F",SUM(VAL_PREMIO)) AS SUM_VAL_PREMIO
FROM 
    (SELECT
    CAST(FORMAT_DATE("%Y%m", DAT_EMISSAO_PREMIO) AS INT64) AS ANO_MES_EMISSAO
    ,COD_PREF_EMPRESA
    ,COD_EMPRESA
    ,NUM_REFER_SISFAT
    ,NUM_REF
    ,IDT_STATUS_FATURA
    ,DAT_MANUT
    ,DAT_EMISSAO_PREMIO
    ,TMP_COMMIT_ORIGEM_CDC
    ,COD_FATURA_COMPL
    ,COD_PLANO
    ,VAL_PREMIO
    ,DENSE_RANK() OVER (PARTITION BY COD_ACUMULA_FATURA,COD_PREF_EMPRESA,COD_EMPRESA,NUM_REFER_SISFAT,COD_FATURA_COMPL,COD_PLANO ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) RANKING_A
    FROM `OW_LAND_SAU_PRD.ACUMULA_FATURA`) AS A
  LEFT OUTER JOIN 
      (SELECT 
	    COD_EMPRESA
		  ,COD_PREF_EMPRESA
		  ,DSC_RAZAO_SOCIAL
		  ,FLG_CONTR_ADESAO
		  ,COD_CIA
      ,COD_PROD
      ,TMP_COMMIT_ORIGEM_CDC
      ,FLG_COPART_REVERS
      ,DENSE_RANK() OVER (PARTITION BY COD_PREF_EMPRESA, COD_EMPRESA ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) RANK_EMP
	    FROM `OW_LAND_SAU_PRD.EMPRESAS`) AS E
      ON  A.COD_PREF_EMPRESA = E.COD_PREF_EMPRESA
      AND A.COD_EMPRESA = E.COD_EMPRESA
 LEFT OUTER JOIN
        (SELECT 
		    COD_PROD
        ,COD_TIPO_PRODUTO
        ,DSC_PRODUTO
        ,TMP_COMMIT_ORIGEM_CDC
        ,DENSE_RANK() OVER (PARTITION BY COD_PROD ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) RANK_PROD
        FROM `OW_LAND_SAU_PRD.PRODUTO_SAUDE`) AS PS
        ON E.COD_PROD = PS.COD_PROD
 LEFT OUTER JOIN 
	(SELECT 
		   COD_PREF_EMPRESA
		 , COD_EMPRESA
	   , COD_CATEGORIA
	   , NME_CATEGORIA
	   , DENSE_RANK() OVER (PARTITION BY COD_PREF_EMPRESA, COD_EMPRESA, COD_CATEGORIA  ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) RANK_CATEM
	FROM OW_LAND_SAU_PRD.CATEGORIA_EMPRESA)CE
	   ON A.COD_EMPRESA = CE.COD_EMPRESA
	  AND A.COD_PREF_EMPRESA = CE.COD_PREF_EMPRESA
	  AND A.COD_PLANO = CE.COD_CATEGORIA
	  and CE.RANK_CATEM = 1        
        
LEFT JOIN
    (SELECT PM.COD_ESPECIFICACAO,
        MAX(CASE WHEN PM.COD_ROT_PESQ_UNID IN (100,101,105,106) and VAL_PARTIC_FUNC > 0  THEN 'S'  
             WHEN PM.COD_ROT_PESQ_UNID = 0 AND AC.COD_ROT_CALC_ACUMU = '801' AND AC.COD_UNIDADE_LIMIT = '106' THEN 'S'
             ELSE 'N' END) AS COPART    
 FROM OW_LAND_SAU_PRD.PLANO_MEDICO PM
 LEFT JOIN OW_LAND_SAU_PRD.PLANO_ACUM_ESPEC AC
   ON cAST(AC.COD_PLANO AS STRING) = CAST(PM.COD_ESPECIFICACAO  AS STRING)
  AND CAST(AC.COD_GRP_ESTAT_INIC AS STRING) = CAST(PM.COD_GRP_ESTAT_INIC AS STRING)
  AND CAST(AC.COD_GRP_ESTAT_FIM  AS STRING) = CAST(PM.COD_GRP_ESTAT_FIM  AS STRING)
 WHERE PM.DAT_FIM_VIG_ESP >= current_DATE
   and PM.COD_GRP_ESTAT_INIC NOT IN (4164,4165) 
   and PM.COD_GRP_ESTAT_FIM NOT IN (4164,4165) 
 GROUP BY 1 ) PM
ON  PM.COD_ESPECIFICACAO = A.COD_PLANO   
LEFT OUTER JOIN OW_LAND_SAU_PRD.CATEGORIA_ATU CA
  ON TRIM(CE.NME_CATEGORIA) = TRIM(CA.NME_CATEGORIA) 
WHERE A.IDT_STATUS_FATURA IN (50,55,90,95)
AND A.NUM_REFER_SISFAT > 0
AND A.NUM_REF != 0
AND CAST(FORMAT_DATE("%Y%m", A.DAT_EMISSAO_PREMIO) AS INT64) >= 201803
AND CAST(FORMAT_DATE("%Y%m", A.DAT_EMISSAO_PREMIO) AS INT64) <= 201902
AND RANKING_A = 1 AND RANK_EMP = 1 AND RANK_PROD = 1
GROUP BY 1,2,3,4,5,6,7,8,9

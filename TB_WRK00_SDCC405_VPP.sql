
 CREATE OR REPLACE TABLE `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK00_SDCC405_VPP` AS


WITH T_SOLIC_VPP AS
    (SELECT * EXCEPT(R_RNK)
     FROM
         (SELECT NUM_SOLICITACAO_VPP,
                 CONCAT(COD_PREF_EMPRESA,COD_EMPRESA,COD_FAMILIAR_BENEF,COD_RDP) COD_BENEFICIARIO_CHAVE,
                 DAT_INCLUSAO,
                 IDC_TIPO_SOLICITACAO,
                 COD_PRESTADOR,
                 NUM_STATUS,
                 NUM_GUIA_PRESTADOR,
                 NUM_SOLICITACAO_VPP_REF,
                 DAT_LIBERACAO_NEGOC,
                 TMP_COMMIT_ORIGEM_CDC,
                 ROW_NUMBER() OVER(PARTITION BY NUM_SOLICITACAO_VPP
                                   ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) R_RNK
          FROM `self-service-saude.OW_LAND_SAU_PRD.SOLICITACAO_VPP`)
     WHERE R_RNK = 1)


,T_BENEF AS (
            SELECT * EXCEPT(R_RNK) 
            FROM 
                (SELECT 
                   COD_ANALITICO_BENEFICIARIO,
                   COD_CARTEIRINHA_BENEFICIARIO,
                   NME_BENEFICIARIO,
                   NUM_CPF,
                   COD_PRODUTO,
                   DSC_PRODUTO,
                   COD_SEXO,
                   DSC_SEXO,
                   EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM DAT_NASCIMENTO) NUM_IDADE,
                   COD_PLANO,
                   NME_PLANO,
                   DAT_EXCLUSAO_OPERACIONAL,
                   COD_MOTIVO_EXCLUSAO,
                   DSC_MOTIVO_EXCLUSAO,
                   ROW_NUMBER() OVER(PARTITION BY COD_ANALITICO_BENEFICIARIO) R_RNK
                   FROM `self-service-saude.TRU_SAUDE_OPERACAO_PRD.TB_CAD_BENEFICIARIO`) WHERE R_RNK = 1)


,UF_BENEFICIARIO AS (
      SELECT * EXCEPT (R_RNK) 
      FROM 
      (SELECT 
      COD_CARTEIRINHA,
      NME_MUNICIPIO_LOCAL,
      SIG_UF_LOCAL,
      NUM_CEP_COMPLETO_TITULAR,
      COD_PREF_EMPRESA,
      COD_EMPRESA,
      COD_FAMILIAR_BENEF,
      COD_RDP,
      ROW_NUMBER() OVER(PARTITION BY COD_CARTEIRINHA) R_RNK

FROM `self-service-saude.VW_CONS_SAU.TB_DIM_BENEFICIARIO`) WHERE R_RNK = 1)


,PRESTADOR AS (
SELECT * EXCEPT(R_RNK) 
FROM (
SELECT 
 COD_PRESTADOR
,NME_FANTASIA
,SIG_UF 
,NME_MUNICIPIO 
,ROW_NUMBER() OVER(PARTITION BY COD_PRESTADOR) R_RNK
FROM `self-service-saude.VW_CONS_SAU.TB_DIM_PRESTADOR`) WHERE R_RNK = 1)


,SERVICOS AS (
SELECT DISTINCT
 COD_SERVICO
,DSC_SERVICO_COMPL DSC_SERVICO
FROM `self-service-saude.VW_CONS_SAU.TB_DIM_SERVICO`)


,ITEM_INTERNACAO AS (
SELECT 
A.* EXCEPT (R_RNK),
B.DSC_SERVICO
FROM (
SELECT 
NUM_SOLIC_INTERNACAO_ITEM, 
CAST(NUM_SOLICITACAO_VPP AS INT64) NUM_SOLICITACAO_VPP,
CAST(COD_SERVICO AS INT64) COD_SERVICO,
IDC_STATUS_ITEM,
ROW_NUMBER() OVER(PARTITION BY NUM_SOLIC_INTERNACAO_ITEM,  NUM_SOLICITACAO_VPP ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) R_RNK


FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_INTERNACAO_ITEM`) A
LEFT JOIN SERVICOS B
ON A.COD_SERVICO = B.COD_SERVICO

WHERE R_RNK = 1
AND IDC_STATUS_ITEM IN ('1','3'))


,T_INTERNACAO AS
    ( SELECT 
         INTERNACAO.* 
        ,ITEM_INTER.NUM_SOLIC_INTERNACAO_ITEM
        ,ITEM_INTER.COD_SERVICO
        ,ITEM_INTER.DSC_SERVICO
        FROM (
        SELECT * EXCEPT(R_RNK) FROM (
        SELECT
            NUM_SOLICITACAO_VPP,
            NUM_DOC_CONTRAT_SOLIC,
            IDC_TIPO_DOC_CONTRAT_SOLIC,
            NME_CONTRAT_SOLIC,
            NME_PROFISSIONAL_SOLIC,
            NUM_CONS_PROF_SOLIC,
            DAT_ATENDIMENTO,
            NUM_VPP,
            FLG_PREV_USO_QUIMIO,
            ROW_NUMBER() OVER(PARTITION BY NUM_SOLICITACAO_VPP ORDER BY TMP_COMMIT_ORIGEM_CDC DESC ) R_RNK
            FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_INTERNACAO`  )
            WHERE R_RNK = 1) INTERNACAO
            LEFT JOIN ITEM_INTERNACAO ITEM_INTER
            ON INTERNACAO.NUM_SOLICITACAO_VPP = ITEM_INTER.NUM_SOLICITACAO_VPP)


, ITEM_SP_SOLIC AS (

            SELECT 
             SP.* 
            ,SERV.DSC_SERVICO
            FROM (

            SELECT * EXCEPT (R_RNK) FROM (
            SELECT 

            NUM_SOLIC_SP_SADT_ITEM, 
            CAST(NUM_SOLICITACAO_VPP AS INT64) NUM_SOLICITACAO_VPP, 
            CAST(COD_SERVICO AS INT64) COD_SERVICO, 
            IDC_STATUS_ITEM,
            ROW_NUMBER() OVER(PARTITION BY NUM_SOLIC_SP_SADT_ITEM, NUM_SOLICITACAO_VPP ORDER BY TMP_COMMIT_ORIGEM_CDC ) R_RNK

            FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_SP_SADT_ITEM`) 
            WHERE R_RNK = 1
            AND IDC_STATUS_ITEM IN ('1','3')) SP
            LEFT JOIN SERVICOS SERV
            ON SP.COD_SERVICO = SERV.COD_SERVICO)   


,T_SOLIC_SP_SADT AS (    
         
         SELECT 
           SP_SADT.*
          ,ITEM_SADT.COD_SERVICO
          ,ITEM_SADT.DSC_SERVICO
          ,ITEM_SADT.NUM_SOLIC_SP_SADT_ITEM

         FROM (
         SELECT * EXCEPT(R_RNK) FROM (
         SELECT 
            NUM_SOLICITACAO_VPP, 
            NUM_DOC_CONTRAT_SOLIC, 
            IDC_TIPO_DOC_CONTRAT_SOLIC, 
            NME_CONTRAT_SOLIC, 
            NME_PROFISSIONAL_SOLIC,
            NUM_CONS_PROF_SOLIC, 
            DAT_ATENDIMENTO, 
            NUM_VPP,
            ROW_NUMBER() OVER(PARTITION BY NUM_SOLICITACAO_VPP
                                   ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) R_RNK
          FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_SP_SADT`)
          WHERE R_RNK = 1 ) SP_SADT
          LEFT JOIN ITEM_SP_SOLIC ITEM_SADT
          ON SP_SADT.NUM_SOLICITACAO_VPP = ITEM_SADT.NUM_SOLICITACAO_VPP)


,QUIMIO_MEDICAMENTO AS (

SELECT * EXCEPT(R_RNK) FROM( 
SELECT 
NUM_SOLIC_QUIMIO_MEDIC, 
NUM_SOLICITACAO_VPP, 
DAT_APLICACAO, 
DAT_AUT_APLICACAO,
ROW_NUMBER() over(partition by NUM_SOLIC_QUIMIO_MEDIC, num_solicitacao_vpp order by TMP_COMMIT_ORIGEM_CDC desc) R_RNK 

FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_QUIMIO_MEDICAMENTO` )
where R_RNK = 1)
         
--JUNÇÃO DOS ITENS DE RADIO COM DESCRITIVO
, ITEM_RADIO AS (
      SELECT 
       RADIO.*
      ,SERV.DSC_SERVICO
      
      FROM  
     (SELECT * EXCEPT(R_RNK) FROM (
      SELECT 
            NUM_SOLIC_RADIO_ITEM,
            CAST(NUM_SOLICITACAO_VPP AS INT64) NUM_SOLICITACAO_VPP ,
            CAST(COD_SERVICO AS INT64) COD_SERVICO,
            IDC_STATUS_ITEM,
            ROW_NUMBER() over(partition BY NUM_SOLIC_RADIO_ITEM, NUM_SOLICITACAO_VPP
                              ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) R_RNK
     FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_RADIOTERAPIA_ITEM`) 
     WHERE R_RNK =1 
     AND IDC_STATUS_ITEM in (1,3)) RADIO 
     LEFT JOIN SERVICOS SERV
     ON RADIO.COD_SERVICO = SERV.COD_SERVICO)


,T_RADIO_ULT AS
    (
      SELECT * EXCEPT(D_RNK)
     FROM
         (
            SELECT 
                 NUM_SOLICITACAO_VPP,
                 NME_PROFISSIONAL_SOLIC,
                 DSC_EMAIL_PROF_SOLIC,
                 NUM_TEL_PROF_SOLIC,
                 NUM_ECOG,
                 DAT_DIAGNOSTICO,
                 DSC_DIAG_CITO_HISTO,
                 NUM_FINALIDAD_TRAT,
                 NUM_ESTAD_TUMOR,
                 DAT_SOLICITACAO,
                 NUM_VPP,
                 NUM_DIAG_IMAGEM,
                 DSC_CIRURGIA,
                 DAT_REALIZACAO_CIRURGIA,
                 DSC_QUIMIOTERAPIA,
                 DAT_ULTIMA_QUIMIO,
                 NUM_CAMPO_IRRADIACAO,
                 DSC_OBS_JUSTIF_SAS,
                 NUM_CAMPO_IRRAD_AUTZ,
                 DENSE_RANK() OVER(PARTITION BY NUM_SOLICITACAO_VPP
                                   ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) D_RNK
          FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_RADIOTERAPIA`)
     WHERE D_RNK = 1)
      

,T_RADIO AS
    (SELECT 
            A.NUM_SOLICITACAO_VPP,
            A.DAT_SOLICITACAO,
            A.NME_PROFISSIONAL_SOLIC,
            A.DSC_EMAIL_PROF_SOLIC,
            A.NUM_TEL_PROF_SOLIC,
            E.COD_DIAG_IMAGEM_ANS,
            E.DSC_DIAG_IMAGEM,
            DATE('1900-01-01') AS DAT_AUTORIZACAO,
            A.DAT_REALIZACAO_CIRURGIA,
            A.DSC_CIRURGIA,
            A.DSC_QUIMIOTERAPIA,
            A.DAT_ULTIMA_QUIMIO,
            A.NUM_CAMPO_IRRADIACAO,
            A.NUM_CAMPO_IRRAD_AUTZ,
            "" AS DSC_AREA_IRRADIADA,
            DATE('1900-01-01') AS DAT_ULTIMA_RADIO,
            "" AS DSC_TIPO_QUIMIO,
            B.COD_ECOG_ANS,
            B.DSC_ECOG,
            A.DAT_DIAGNOSTICO,
            A.DSC_DIAG_CITO_HISTO,
            NULL AS NUM_CICLO_PREVISTO,
            NULL AS NUM_INTERVALO_CICLO,
            NULL AS NUM_CICLO_ATUAL,
            D.COD_FINALIDAD_TRAT_ANS,
            D.DSC_FINALIDAD_TRAT,
            C.COD_ESTAD_TUMOR_ANS,
            C.DSC_ESTAD_TUMOR,
            "" AS DSC_PLANO_TERAPEUTICO,
            A.NUM_VPP,
            A.DSC_OBS_JUSTIF_SAS DSC_OBS_JUSTIFICATIVA_SAS,
            RADIO_CID.CID
            -- "RADIO" AS TIPO_TABELA
     
     FROM T_RADIO_ULT A
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_CAPAC_FUNC_PACIENTE` B 
     ON A.NUM_ECOG = B.NUM_ECOG
     
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_ESTADIAMENTO_TUMOR` C 
     ON A.NUM_ESTAD_TUMOR = C.NUM_ESTAD_TUMOR
     
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_FINALIDADE_TRATAMENTO` D 
     ON A.NUM_FINALIDAD_TRAT = D.NUM_FINALIDAD_TRAT

     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_DIAGNOSTICO_IMAGEM` E
     ON A.NUM_DIAG_IMAGEM = E.NUM_DIAG_IMAGEM

     LEFT JOIN (SELECT CID, NUM_SOLICITACAO_VPP FROM (
           SELECT  
           NUM_SOLICITACAO_VPP, 
           NUM_SEQUENCIA, 
           COD_INTER_DOENCA CID,
           ROW_NUMBER() OVER(PARTITION BY NUM_SOLICITACAO_VPP, NUM_SEQUENCIA ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) R_RNK
           FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_RADIOTERAPIA_CID`) WHERE R_RNK = 1 ) RADIO_CID
     ON A.NUM_SOLICITACAO_VPP = RADIO_CID.NUM_SOLICITACAO_VPP

     )      


    ,T_QUIMIO_ULT AS ( 
SELECT  * EXCEPT(R_RNK) FROM (
  SELECT 
                 NUM_SOLICITACAO_VPP,
                 DAT_SOLICITACAO,
                 DAT_AUTORIZACAO,
                 NME_PROFISSIONAL_SOLIC,
                 DSC_EMAIL_PROF_SOLIC,
                 NUM_TEL_PROF_SOLIC,
                 DAT_DIAGNOSTICO,
                 NUM_ECOG,
                 DSC_DIAG_CITO_HISTO,
                 NUM_FINALIDAD_TRAT,
                 NUM_ESTAD_TUMOR,
                 NUM_VPP,
                 NUM_CICLO_ATUAL,
                 NUM_DIAG_IMAGEM,
                 DSC_CIRURGIA,
                 DAT_REALIZACAO,
                 DSC_AREA_IRRADIADA,
                 DAT_ULTIMA_RADIO,
                 DSC_OBS_JUSTIFICATIVA_SAS,
                 NUM_CICLO_PREVISTO,
                 NUM_INTERVALO_CICLO,
                 DSC_PLANO_TERAPEUTICO,
                 NUM_TIPO_QUIMIO,
                 ROW_NUMBER() OVER(PARTITION BY NUM_SOLICITACAO_VPP ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) R_RNK

                 FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_QUIMIOTERAPIA` ) WHERE R_RNK = 1)


    ,T_QUIMIO AS
    ( SELECT A.NUM_SOLICITACAO_VPP,
             A.DAT_SOLICITACAO,
             A.NME_PROFISSIONAL_SOLIC,
             A.DSC_EMAIL_PROF_SOLIC,
             A.NUM_TEL_PROF_SOLIC,
             E.COD_DIAG_IMAGEM_ANS,
             E.DSC_DIAG_IMAGEM,
             A.DAT_AUTORIZACAO,
             A.DAT_REALIZACAO DAT_REALIZACAO_CIRURGIA,
             A.DSC_CIRURGIA,
             "" AS DSC_QUIMIOTERAPIA,
             date('1900-01-01') AS DAT_ULTIMA_QUIMIO,
             null AS NUM_CAMPO_IRRADIACAO,
             null AS NUM_CAMPO_IRRAD_AUTZ,
             A.DSC_AREA_IRRADIADA,
             A.DAT_ULTIMA_RADIO,
             TIPO_QUIMIO.DSC_TIPO_QUIMIO,
             B.COD_ECOG_ANS,
             B.DSC_ECOG,
             A.DAT_DIAGNOSTICO,
             A.DSC_DIAG_CITO_HISTO,
             A.NUM_CICLO_PREVISTO,
             A.NUM_INTERVALO_CICLO,
             A.NUM_CICLO_ATUAL,
             D.COD_FINALIDAD_TRAT_ANS,
             D.DSC_FINALIDAD_TRAT,
             C.COD_ESTAD_TUMOR_ANS,
             C.DSC_ESTAD_TUMOR,
             A.DSC_PLANO_TERAPEUTICO,
             A.NUM_VPP,
             A.DSC_OBS_JUSTIFICATIVA_SAS,
             QUIMIO_CID.CID, 
            

     FROM T_QUIMIO_ULT A
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_CAPAC_FUNC_PACIENTE` B 
     ON A.NUM_ECOG = B.NUM_ECOG
     
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_ESTADIAMENTO_TUMOR` C
     ON A.NUM_ESTAD_TUMOR = C.NUM_ESTAD_TUMOR
     
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_FINALIDADE_TRATAMENTO` D 
     ON A.NUM_FINALIDAD_TRAT = D.NUM_FINALIDAD_TRAT

     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_DIAGNOSTICO_IMAGEM` E
     ON A.NUM_DIAG_IMAGEM = E.NUM_DIAG_IMAGEM 

     LEFT JOIN (SELECT NUM_SOLICITACAO_VPP, CID FROM(
                SELECT 
                NUM_SOLICITACAO_VPP, 
                NUM_SEQUENCIA, 
                COD_INTER_DOENCA CID,
                DENSE_RANK() OVER(PARTITION BY NUM_SOLICITACAO_VPP, NUM_SEQUENCIA ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) D_RNK
                FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_QUIMIOTERAPIA_CID` ) WHERE D_RNK = 1 )QUIMIO_CID
     ON A.NUM_SOLICITACAO_VPP = QUIMIO_CID.NUM_SOLICITACAO_VPP


     LEFT JOIN  `self-service-saude.OW_LAND_SAU_PRD.TISS_TIPO_QUIMIOTERAPIA` TIPO_QUIMIO
     ON A.NUM_TIPO_QUIMIO = TIPO_QUIMIO.NUM_TIPO_QUIMIO)


    ,T_RADIO_QUIMIO AS (

    SELECT * FROM T_QUIMIO
    UNION ALL
    SELECT * FROM T_RADIO)
--junção da tabela de radio e quimio com os medicamentos/data de aplicação de medicamentos de quimio 20/08/2020
-- junção itens radioterapia 21/08/2020
, T_RADIO_QUIMIO_MED AS (
SELECT DISTINCT 
A.*,
B.DAT_APLICACAO,
B.DAT_AUT_APLICACAO,
B.NUM_SOLIC_QUIMIO_MEDIC,
I_R.NUM_SOLIC_RADIO_ITEM,
I_R.COD_SERVICO,
I_R.DSC_SERVICO

FROM T_RADIO_QUIMIO A
LEFT JOIN QUIMIO_MEDICAMENTO B
ON A.NUM_SOLICITACAO_VPP = B.NUM_SOLICITACAO_VPP

LEFT JOIN ITEM_RADIO I_R
ON A.NUM_SOLICITACAO_VPP = I_R.NUM_SOLICITACAO_VPP)


, CTE AS (


SELECT 

                 A.COD_BENEFICIARIO_CHAVE,
                 BENEF.COD_CARTEIRINHA_BENEFICIARIO, 
                 BENEF.NME_BENEFICIARIO,
                 BENEF.NUM_CPF,
                 BENEF.COD_PRODUTO, 
                 BENEF.DSC_PRODUTO,
                 BENEF.COD_PLANO, 
                 BENEF.NME_PLANO,
                 BENEF.COD_SEXO,
                 BENEF.DSC_SEXO,
                 BENEF.NUM_IDADE,
                 UF_BENEF.NME_MUNICIPIO_LOCAL,
                 UF_BENEF.SIG_UF_LOCAL,
                 UF_BENEF.NUM_CEP_COMPLETO_TITULAR NUM_CEP_BENEF,
                 MDM_TEL.NUM_TELEFONE,
                 MDM_EMAIL.DSC_EMAIL,
                 BENEF.DAT_EXCLUSAO_OPERACIONAL DAT_EXCLUSAO, 
                 BENEF.COD_MOTIVO_EXCLUSAO, 
                 BENEF.DSC_MOTIVO_EXCLUSAO,
                 A.NUM_SOLICITACAO_VPP,
                 A.DAT_INCLUSAO,
                 D.DAT_SOLICITACAO,
                 A.IDC_TIPO_SOLICITACAO,
                 CASE A.IDC_TIPO_SOLICITACAO
                    WHEN 0
                    THEN 'SP SADT'

                    WHEN 1
                    THEN 'INTERNACAO'

                    WHEN 2
                    THEN 'PRORROGACAO'

                    WHEN 3
                    THEN 'OPME'

                    WHEN 4
                    THEN 'RADIOTERAPIA'

                    WHEN 5
                    THEN 'QUIMIOTERAPIA'

                    END DSC_TIPO_SOLICITACAO,

                 A.COD_PRESTADOR,
                 PREST.NME_FANTASIA,
                 PREST.SIG_UF SIG_UF_PRESTADOR,
                 PREST.NME_MUNICIPIO MUNICIPIO_PRESTADOR,
                 A.NUM_STATUS,
                 A.NUM_GUIA_PRESTADOR,
                 A.NUM_SOLICITACAO_VPP_REF,
                 A.DAT_LIBERACAO_NEGOC,
                 -- IDC_TIPO_SOLICITACAO IDENTIFICA DE ONDE É A INTERNAÇÃO, 0 SP_SADT, 1 INTERNACAO, 4 RADIOTERAPIA, 5 QUIMIOTERAPIA
                 
                 CASE A.IDC_TIPO_SOLICITACAO
                    WHEN 0
                    THEN C.NUM_DOC_CONTRAT_SOLIC

                    ELSE B.NUM_DOC_CONTRAT_SOLIC
                       END NUM_DOC_CONTRAT_SOLIC,


                 CASE A.IDC_TIPO_SOLICITACAO
                    WHEN 0 
                    THEN C.IDC_TIPO_DOC_CONTRAT_SOLIC

                    ELSE B.IDC_TIPO_DOC_CONTRAT_SOLIC
                       END IDC_TIPO_DOC_CONTRAT_SOLIC,


                 CASE A.IDC_TIPO_SOLICITACAO
                    WHEN 0
                    THEN C.NME_CONTRAT_SOLIC

                    ELSE B.NME_CONTRAT_SOLIC
                       END NME_CONTRAT_SOLIC,


                 CASE A.IDC_TIPO_SOLICITACAO
                    WHEN 0 
                    THEN C.NME_PROFISSIONAL_SOLIC

                    WHEN 1
                    THEN B.NME_PROFISSIONAL_SOLIC

                    WHEN 4
                    THEN D.NME_PROFISSIONAL_SOLIC

                    WHEN 5
                    THEN D.NME_PROFISSIONAL_SOLIC

                    ELSE B.NME_PROFISSIONAL_SOLIC
                       END NME_PROFISSIONAL_SOLIC,


                 CASE A.IDC_TIPO_SOLICITACAO
                    WHEN 0
                    THEN C.NUM_CONS_PROF_SOLIC

                    ELSE B.NUM_CONS_PROF_SOLIC

                    END CRM_SOLIC,

                
                 D.DSC_EMAIL_PROF_SOLIC,
                 D.NUM_TEL_PROF_SOLIC,
                 B.FLG_PREV_USO_QUIMIO,

                 CASE A.IDC_TIPO_SOLICITACAO 
                    WHEN 0
                    THEN C.NUM_VPP
                    
                    WHEN 1
                    THEN B.NUM_VPP
                   
                    WHEN 4
                    THEN D.NUM_VPP
                    
                    WHEN 5                                  
                    THEN D.NUM_VPP
                    
                    ELSE B.NUM_VPP
                   
                       END NUM_VPP,

                 C.NUM_SOLIC_SP_SADT_ITEM,
                 D.NUM_SOLIC_QUIMIO_MEDIC,
                 D.NUM_SOLIC_RADIO_ITEM,
                 B.NUM_SOLIC_INTERNACAO_ITEM,
                 

                 CASE WHEN D.NUM_SOLIC_RADIO_ITEM IS NOT NULL
                      THEN D.COD_SERVICO

                      WHEN B.NUM_SOLIC_INTERNACAO_ITEM IS NOT NULL
                      THEN B.COD_SERVICO

                      WHEN C.NUM_SOLIC_SP_SADT_ITEM IS NOT NULL
                      THEN C.COD_SERVICO

                      ELSE NULL
                      END COD_SERVICO,

                
                 CASE WHEN D.NUM_SOLIC_RADIO_ITEM IS NOT NULL
                      THEN D.DSC_SERVICO

                      WHEN B.NUM_SOLIC_INTERNACAO_ITEM IS NOT NULL
                      THEN B.DSC_SERVICO

                      WHEN C.NUM_SOLIC_SP_SADT_ITEM IS NOT NULL
                      THEN C.DSC_SERVICO

                      ELSE NULL

                      END DSC_SERVICO,


                 D.DAT_APLICACAO,
                 D.DAT_AUT_APLICACAO,
                 CASE D.DAT_AUTORIZACAO
                    WHEN ('1900-01-01')
                    THEN NULL 

                    ELSE D.DAT_AUTORIZACAO
                    END DAT_AUTORIZACAO,

                 CASE A.IDC_TIPO_SOLICITACAO
                    WHEN 0
                    THEN C.DAT_ATENDIMENTO

                    ELSE B.DAT_ATENDIMENTO
                       END DAT_ATENDIMENTO,

                 D.COD_DIAG_IMAGEM_ANS,
                 D.DSC_DIAG_IMAGEM,
                 D.DAT_REALIZACAO_CIRURGIA,
                 D.DSC_CIRURGIA,
                 D.DSC_QUIMIOTERAPIA,
                 
                 CASE D.DAT_ULTIMA_QUIMIO
                    WHEN ('1900-01-01')
                    THEN NULL
                    
                    ELSE D.DAT_ULTIMA_QUIMIO
                    END DAT_ULTIMA_QUIMIO,

                 D.NUM_CAMPO_IRRADIACAO,
                 D.NUM_CAMPO_IRRAD_AUTZ,
                 D.DSC_AREA_IRRADIADA,
                 CASE D.DAT_ULTIMA_RADIO
                    WHEN ('1900-01-01')
                    THEN NULL
                    
                    ELSE D.DAT_ULTIMA_RADIO
                    END DAT_ULTIMA_RADIO,

                 D.COD_ECOG_ANS,
                 D.DSC_ECOG,
                 D.DAT_DIAGNOSTICO,
                 D.DSC_DIAG_CITO_HISTO,
                 D.NUM_CICLO_PREVISTO,
                 D.NUM_INTERVALO_CICLO,
                 D.NUM_CICLO_ATUAL,
                 CASE D.NUM_CICLO_ATUAL WHEN 1 THEN 'S'
                 ELSE 'N' END FLG_PRIMEIRO_CICLO,
                 D.COD_FINALIDAD_TRAT_ANS,
                 D.DSC_FINALIDAD_TRAT,
                 D.COD_ESTAD_TUMOR_ANS,
                 D.DSC_ESTAD_TUMOR,
                 D.DSC_PLANO_TERAPEUTICO,
                 D.DSC_OBS_JUSTIFICATIVA_SAS,
                 D.CID,
                 IF(B.NUM_VPP = D.NUM_VPP,1,0)FLG_QUIMIO_INTER
                 



FROM T_SOLIC_VPP A
LEFT JOIN T_INTERNACAO B
ON A.NUM_SOLICITACAO_VPP = B.NUM_SOLICITACAO_VPP

LEFT JOIN T_SOLIC_SP_SADT C
ON A.NUM_SOLICITACAO_VPP = C.NUM_SOLICITACAO_VPP

LEFT JOIN T_RADIO_QUIMIO_MED D
ON A.NUM_SOLICITACAO_VPP = D.NUM_SOLICITACAO_VPP

LEFT JOIN T_BENEF BENEF
ON A.COD_BENEFICIARIO_CHAVE = BENEF.COD_ANALITICO_BENEFICIARIO

LEFT JOIN (SELECT NUM_CPF_CNPJ,
            CONCAT("(",NUM_DDD_TEL,")"," ",NUM_TELEFONE) NUM_TELEFONE
FROM (SELECT NUM_CPF_CNPJ,
             NUM_DDD_TEL,
             NUM_TELEFONE,
             ROW_NUMBER() OVER(PARTITION BY NUM_CPF_CNPJ ORDER BY DAT_ULT_ALT_TEL DESC) D_RNK
FROM `self-service-saude.SBX_ESTRATIFICACAO.TB_DIM_ULT_VERSAO_TELEFONE_MDM_API`)
     WHERE D_RNK = 1) MDM_TEL
ON BENEF.NUM_CPF = MDM_TEL.NUM_CPF_CNPJ

LEFT JOIN (SELECT *
FROM (SELECT NUM_CPF_CNPJ,
             DSC_EMAIL,
             ROW_NUMBER() OVER(PARTITION BY NUM_CPF_CNPJ ORDER BY DAT_ULT_ALT_EMAIL DESC) D_RNK
FROM `self-service-saude.SBX_ESTRATIFICACAO.TB_DIM_ULT_VERSAO_EMAIL_MDM_API`)
     WHERE D_RNK = 1) MDM_EMAIL
ON BENEF.NUM_CPF = MDM_EMAIL.NUM_CPF_CNPJ

LEFT JOIN UF_BENEFICIARIO UF_BENEF
ON BENEF.COD_ANALITICO_BENEFICIARIO = CONCAT(UF_BENEF.COD_PREF_EMPRESA,UF_BENEF.COD_EMPRESA,UF_BENEF.COD_FAMILIAR_BENEF,UF_BENEF.COD_RDP)

LEFT JOIN PRESTADOR PREST
ON A.COD_PRESTADOR = PREST.COD_PRESTADOR)

SELECT 
*
 FROM CTE 
-- where NUM_VPP in (SELECT DISTINCT NUM_VPP FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS`)
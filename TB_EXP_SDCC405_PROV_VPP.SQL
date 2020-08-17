
create or replace table `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_SDCC405_PROV_VPP` AS

WITH T_SOLIC_VPP AS
    (SELECT *
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
                 DENSE_RANK() OVER(PARTITION BY NUM_SOLICITACAO_VPP
                                   ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) D_RNK
          FROM `self-service-saude.OW_LAND_SAU_PRD.SOLICITACAO_VPP`)
     WHERE D_RNK = 1),
     
     T_INTERNACAO AS
    ( 
        SELECT * EXCEPT(D_RNK) FROM(
      SELECT 
            NUM_SOLICITACAO_VPP,
            NUM_DOC_CONTRAT_SOLIC,
            IDC_TIPO_DOC_CONTRAT_SOLIC,
            NME_CONTRAT_SOLIC,
            NME_PROFISSIONAL_SOLIC,
            DAT_ATENDIMENTO,
            NUM_VPP,
            DENSE_RANK() OVER(PARTITION BY NUM_SOLICITACAO_VPP ORDER BY TMP_COMMIT_ORIGEM_CDC DESC ) D_RNK
            FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_INTERNACAO`)
        WHERE D_RNK = 1),
     
     T_SOLIC_SP_SADT AS
    (
      SELECT * EXCEPT(D_RNK)
     FROM
         (SELECT 
            NUM_SOLICITACAO_VPP, 
            NUM_DOC_CONTRAT_SOLIC, 
            IDC_TIPO_DOC_CONTRAT_SOLIC, 
            NME_CONTRAT_SOLIC, 
            NME_PROFISSIONAL_SOLIC, 
            DAT_ATENDIMENTO, 
            NUM_VPP,
            DENSE_RANK() OVER(PARTITION BY NUM_SOLICITACAO_VPP
                                   ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) D_RNK
          FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_SP_SADT`)
     WHERE D_RNK = 1 ),
     

     
     T_RADIO_ULT AS
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
                 DENSE_RANK() OVER(PARTITION BY NUM_SOLICITACAO_VPP
                                   ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) D_RNK
          FROM `self-service-saude.OW_LAND_SAU_PRD.SOLIC_RADIOTERAPIA`)
     WHERE D_RNK = 1),
     
     T_RADIO AS
    (SELECT A.NUM_SOLICITACAO_VPP,
            A.DAT_SOLICITACAO,
            A.NME_PROFISSIONAL_SOLIC,
            A.DSC_EMAIL_PROF_SOLIC,
            A.NUM_TEL_PROF_SOLIC,
            B.COD_ECOG_ANS,
            B.DSC_ECOG,
            A.DAT_DIAGNOSTICO,
            A.DSC_DIAG_CITO_HISTO,
            NULL AS NUM_CICLO_ATUAL,
            D.COD_FINALIDAD_TRAT_ANS,
            D.DSC_FINALIDAD_TRAT,
            C.COD_ESTAD_TUMOR_ANS,
            C.DSC_ESTAD_TUMOR,
            A.NUM_VPP
     
     FROM T_RADIO_ULT A
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_CAPAC_FUNC_PACIENTE` B 
     ON A.NUM_ECOG = B.NUM_ECOG
     
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_ESTADIAMENTO_TUMOR` C 
     ON A.NUM_ESTAD_TUMOR = C.NUM_ESTAD_TUMOR
     
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_FINALIDADE_TRATAMENTO` D 
     ON A.NUM_FINALIDAD_TRAT = D.NUM_FINALIDAD_TRAT),
    

T_QUIMIO_ULT AS ( 
SELECT * FROM (
  SELECT 
                 NUM_SOLICITACAO_VPP,
                 DAT_SOLICITACAO,
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
                 DENSE_RANK() OVER(PARTITION BY NUM_SOLICITACAO_VPP ORDER BY TMP_COMMIT_ORIGEM_CDC DESC) D_RNK

                 FROM `self-service-saude.RAW_SAUDE_PRD.SOLIC_QUIMIOTERAPIA` ) WHERE D_RNK = 1),

    T_QUIMIO AS
    ( SELECT A.NUM_SOLICITACAO_VPP,
             A.DAT_SOLICITACAO,
             A.NME_PROFISSIONAL_SOLIC,
             A.DSC_EMAIL_PROF_SOLIC,
             A.NUM_TEL_PROF_SOLIC,
             B.COD_ECOG_ANS,
             B.DSC_ECOG,
             A.DAT_DIAGNOSTICO,
             A.DSC_DIAG_CITO_HISTO,
             A.NUM_CICLO_ATUAL,
             D.COD_FINALIDAD_TRAT_ANS,
             D.DSC_FINALIDAD_TRAT,
             C.COD_ESTAD_TUMOR_ANS,
             C.DSC_ESTAD_TUMOR,
             A.NUM_VPP
     FROM T_QUIMIO_ULT A
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_CAPAC_FUNC_PACIENTE` B 
     ON A.NUM_ECOG = B.NUM_ECOG
     
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_ESTADIAMENTO_TUMOR` C
     ON A.NUM_ESTAD_TUMOR = C.NUM_ESTAD_TUMOR
     
     LEFT JOIN `self-service-saude.RAW_SAUDE_PRD.TISS_FINALIDADE_TRATAMENTO` D 
     ON A.NUM_FINALIDAD_TRAT = D.NUM_FINALIDAD_TRAT), 

T_RADIO_QUIMIO AS (

    SELECT * FROM T_QUIMIO
    UNION ALL
    SELECT * FROM T_RADIO 






)
-- select * from T_RADIO_QUIMIO
-- where num_solicitacao_vpp in (SELECT num_solicitacao_vpp FROM `self-service-saude.RAW_SAUDE_PRD.SOLIC_QUIMIOTERAPIA` 
-- where num_vpp in ('5000535864'))
, CTE AS (


SELECT 

                 A.NUM_SOLICITACAO_VPP,
                 A.COD_BENEFICIARIO_CHAVE,
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
                 D.DSC_EMAIL_PROF_SOLIC,
                 D.NUM_TEL_PROF_SOLIC,

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
                 
                 CASE A.IDC_TIPO_SOLICITACAO
                    WHEN 0
                    THEN C.DAT_ATENDIMENTO

                    ELSE B.DAT_ATENDIMENTO
                       END DAT_ATENDIMENTO,

                 D.COD_ECOG_ANS,
                 D.DSC_ECOG,
                 D.DAT_DIAGNOSTICO,
                 D.DSC_DIAG_CITO_HISTO,
                 D.NUM_CICLO_ATUAL,
                 CASE D.NUM_CICLO_ATUAL WHEN 1 THEN 'S'
                 ELSE 'N' END FLG_PRIMEIRO_CICLO,
                 D.COD_FINALIDAD_TRAT_ANS,
                 D.DSC_FINALIDAD_TRAT,
                 D.COD_ESTAD_TUMOR_ANS,
                 D.DSC_ESTAD_TUMOR


FROM T_SOLIC_VPP A
LEFT JOIN T_INTERNACAO B
ON A.NUM_SOLICITACAO_VPP = B.NUM_SOLICITACAO_VPP

LEFT JOIN T_SOLIC_SP_SADT C
ON A.NUM_SOLICITACAO_VPP = C.NUM_SOLICITACAO_VPP

LEFT JOIN T_RADIO_QUIMIO D
ON A.NUM_SOLICITACAO_VPP = D.NUM_SOLICITACAO_VPP
)

SELECT  * FROM CTE 
where NUM_VPP in (SELECT DISTINCT NUM_VPP FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS`)

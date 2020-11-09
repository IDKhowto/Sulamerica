CREATE OR REPLACE TABLE `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_CLASSES_SINISTRO_ESTUDO_NEURO_NEW` AS



with cyte_matmed as (

    SELECT * FROM (
  SELECT
                      IDT_DOCUMENTO,
                      COD_ORIGEM,
                      IDT_COMPLEMENTO,
                      IDT_SEQ_PAGTO,
                      IDT_ANEXO,
                      IDT_ITEM,
                      IDT_ITEM_MAT_MED,
                      COD_ITEM_MAT_MED,
                      DSC_ITEM_MAT_MED,
                      COD_SERVICO,
                      COD_PREF_EMPRESA,
                      COD_EMPRESA,
                      COD_FAMILIAR_BENEF,
                      COD_RDP,
                      VAL_PAGO,
                      ROW_NUMBER() OVER(PARTITION BY CONCAT(cod_pref_empresa,cod_empresa,COD_FAMILIAR_BENEF, cod_rdp),
                                                     IDT_DOCUMENTO,
                                                     COD_ORIGEM,
                                                     IDT_COMPLEMENTO,
                                                     IDT_SEQ_PAGTO,
                                                     IDT_ANEXO,
                                                     IDT_ITEM,
                                                     IDT_ITEM_MAT_MED) R_RNK
               FROM `self-service-saude.SANDBOX_SUSAU.CONTA_DETALHE_MATMED`)
    WHERE R_RNK = 1)
  , ALTO_CUSTO AS (SELECT DISTINCT COD_BENEFICIARIO_CHAVE FROM `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_TOP`)

 ,cyte as (SELECT
     A.COD_DOCUMENTO_SINISTRO,
     A.COD_COMPLEMENTO_SINISTRO,
     A.COD_SEQUENCIA_SINISTRO,
     A.COD_ORIGEM_PRESTADOR,
     A.COD_ITEM_SERVICO_SINISTRO,
     A.COD_ANEXO_ITEM_SERVICO_SINISTRO,
     A.DSC_CLASSIFICACAO_SINISTRO,
     A.DSC_TIPO_SINISTRO,
     A.FLG_EMERGENCIA,
     A.FLG_INTERNACAO,
     A.FLG_LIMINAR,
     B.NUM_SOLICITACAO_VPP,
     B.DAT_INCLUSAO_VPP,
     B.DAT_AUTORIZACAO_VPP,
     B.NUM_STATUS_SOLICITACAO_VPP,
     B.DSC_STATUS_SOLICITACAO_VPP,
     A.NUM_VPP,
     A.NUM_INTERNACAO,
     A.DAT_ATENDIMENTO_SINISTRO,
     A.DAT_EXECUCAO_SINISTRO,
     A.COD_PRODUTO,
     A.DSC_PRODUTO,
     A.NME_FANTASIA_EMPRESA,
     A.DSC_CARTEIRA_EMPRESA,
     A.COD_PLANO,
     A.NME_PLANO,
     A.SGL_UF_TITULAR_BENEFICIARIO SIG_UF,
     A.NME_MUNICIPIO_TITULAR_BENEFICIARIO,
     A.COD_SISTEMICO_BENEFICIARIO,
     A.COD_ANALITICO_BENEFICIARIO,
     A.COD_CARTEIRINHA_BENEFICIARIO,
     A.NME_BENEFICIARIO,
     A.NUM_IDADE_BENEFICIARIO_SINISTRO,
     A.DSC_SEXO_BENEFICIARIO,
     A.COD_PRESTADOR,
     A.NME_MUNICIPIO_PRESTADOR,
     A.SGL_UF_PRESTADOR,
     A.NME_FANTASIA_PRESTADOR,
     B.DAT_INTERNACAO,
     B.COD_PRINCIPAL_SERVICO,
     B.DSC_PRINCIPAL_SERVICO,
     A.COD_SERVICO,
     A.DSC_SERVICO,
     D.DSC_ITEM_MAT_MED,
     B.DAT_ALTA,
     B.DAT_INICIO_TMI,
     B.DAT_FINAL_TMI,
     B.QTD_DIAS_INTERNADO,
     IFNULL(CASE 
        WHEN A.COD_SERVICO NOT IN (61430056, 61430013)
        THEN A.VLR_PAGO_SERVICO
        ELSE D.VAL_PAGO

        END, A.VLR_PAGO_SERVICO) VLR_PAGO_SERVICO,
     B.VLR_PAGO_INTERNACAO_TOTAL,
     CASE
        WHEN A.COD_SERVICO IN (41101030,
                               41101014,
                               41101057,
                               41101545,
                               96003774,
                               96008199,
                               41101537,
                               36010456)
        THEN 'RESSONANCIA'

        WHEN A.COD_SERVICO IN (41001443,
                               32010230,
                               41001010,
                               34010327,
                               41001370,
                               34010068,
                               34900098,
                               34010343,
                               41001389)
        THEN 'TOMOGRAFIA'

        ELSE '-'
        END FLG_RNM_TOMO,
     CASE
        WHEN D.DSC_ITEM_MAT_MED LIKE ('%LYSE%')
        THEN 1
        ELSE 0
        END FLG_ACTILYSE,

     A.FLG_PS,
     A.COD_CID_PRINCIPAL_SINISTRO,
     A.DSC_CID_PRINCIPAL_SINISTRO
     
     FROM `self-service-saude.TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_ITEM_SERVICO_SINISTRO` A
     LEFT JOIN `self-service-saude.TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_INTERNACAO` B
     ON  A.COD_CARTEIRINHA_BENEFICIARIO = B.COD_CARTEIRINHA_BENEFICIARIO
     AND A.NUM_INTERNACAO = B.NUM_INTERNACAO

     LEFT JOIN `self-service-saude.TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_NAO_INTERNACAO` C
     ON  A.COD_CARTEIRINHA_BENEFICIARIO = C.COD_CARTEIRINHA_BENEFICIARIO
     AND A.DAT_ATENDIMENTO_EFETIVO_SINISTRO = C.DAT_ATENDIMENTO_EFETIVO_SINISTRO
     AND A.COD_SERVICO = C.COD_PRINCIPAL_SERVICO

     LEFT JOIN cyte_matmed D
     ON  A.COD_DOCUMENTO_SINISTRO = D.IDT_DOCUMENTO
     AND A.COD_COMPLEMENTO_SINISTRO = D.IDT_COMPLEMENTO
     AND A.COD_SEQUENCIA_SINISTRO = D.IDT_SEQ_PAGTO
     AND A.COD_ANEXO_ITEM_SERVICO_SINISTRO = D.IDT_ANEXO
     AND A.COD_ITEM_SERVICO_SINISTRO = D.IDT_ITEM
     AND A.COD_SERVICO = D.COD_SERVICO
     AND A.COD_PREFIXO_EMPRESA = D.COD_PREF_EMPRESA
     AND A.COD_SUFIXO_EMPRESA = D.COD_EMPRESA
     AND A.COD_FAMILIAR_BENEFICIARIO = D.COD_FAMILIAR_BENEF
     AND A.COD_RDP_BENEFICIARIO = D.COD_RDP

     INNER JOIN ALTO_CUSTO E
     ON A.COD_SISTEMICO_BENEFICIARIO = E.COD_BENEFICIARIO_CHAVE

WHERE DATE_TRUNC(A.DAT_EXECUCAO_SINISTRO , MONTH) BETWEEN '2017-01-01' AND '2020-06-01' 
    AND (DATE_TRUNC(B.DAT_INTERNACAO, MONTH) BETWEEN  '2017-01-01' AND '2020-06-01' 
    OR  B.DAT_INTERNACAO IS NULL)



     )
     

     , ACTILYSE AS(
     select DISTINCT

     COD_ANALITICO_BENEFICIARIO,
     NUM_VPP,
     FLG_ACTILYSE

     FROM cyte
     WHERE FLG_ACTILYSE IN (1))

--JUNÇÃO SINSITRO E FLG
,cyte_actilyse_sin as (
     SELECT
     A.* EXCEPT(FLG_ACTILYSE),
     
     CASE
        WHEN B.FLG_ACTILYSE IN (1)
        THEN 'S'
        ELSE 'N'
        END FLG_ACTILYSE


     FROM cyte A
     LEFT JOIN ACTILYSE B
     ON  A.COD_ANALITICO_BENEFICIARIO = B.COD_ANALITICO_BENEFICIARIO
     AND A.NUM_VPP = B.NUM_VPP)
,cyte_fin as (
SELECT

*,
CASE
          WHEN COD_PRINCIPAL_SERVICO IN (9000003)
          AND  FLG_RNM_TOMO IN ('RESSONANCIA', 'TOMOGRAFIA') 
          AND  FLG_ACTILYSE IN ('S')
          THEN 'ALTA-ALTA'
          
          WHEN COD_PRINCIPAL_SERVICO IN (9000003)
          AND  FLG_ACTILYSE IN ('S')
          AND FLG_RNM_TOMO NOT IN ('RESSONANCIA', 'TOMOGRAFIA')
          THEN 'ALTA'          
          
          WHEN COD_PRINCIPAL_SERVICO IN (9000003)
          AND  FLG_RNM_TOMO IN ('RESSONANCIA', 'TOMOGRAFIA')
          THEN 'MÉDIA'
          
          
          ELSE '-'

          END CLASSIFICACAO_NEURO


FROM cyte_actilyse_sin)

select * from cyte_fin 
-- where num_vpp in (select  distinct num_vpp from cyte_fin where classificacao_neuro not in ('-'))
-- WHERE COD_CARTEIRINHA_BENEFICIARIO IN ('09001103339400014') group by 1




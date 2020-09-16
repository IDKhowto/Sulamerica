
    CREATE OR REPLACE TABLE `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK00_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS` AS
    
    
    
SELECT 
       A.COD_DOCUMENTO_SINISTRO,
       A.COD_COMPLEMENTO_SINISTRO,
       A.COD_SEQUENCIA_SINISTRO,
       A.COD_ITEM_SERVICO_SINISTRO,
       A.COD_ANEXO_ITEM_SERVICO_SINISTRO,
       A.COD_SUFIXO_BENEFICIARIO,
       A.COD_FAMILIAR_BENEFICIARIO,
       A.COD_RDP_BENEFICIARIO,
       A.COD_SISTEMICO_BENEFICIARIO,
       A.COD_ANALITICO_BENEFICIARIO,
       A.COD_DV_BENEFICIARIO,
       A.COD_CARTEIRINHA_BENEFICIARIO,
       A.NME_BENEFICIARIO,
       A.DAT_NASCIMENTO_BENEFICIARIO,
       A.NUM_IDADE_BENEFICIARIO_SINISTRO,
       A.COD_SEXO_BENEFICIARIO,
       A.DSC_SEXO_BENEFICIARIO,
       A.COD_PLANO,
       A.NME_PLANO,
       A.DSC_CARTEIRA_EMPRESA,
       A.NME_MUNICIPIO_TITULAR_BENEFICIARIO,
       B.NUM_CPF_BENEFICIARIO,
       B.NUM_CPF_TITULAR_BENEFICIARIO,
       A.DSC_SITUACAO_BENEFICIARIO,
       A.SGL_UF_TITULAR_BENEFICIARIO,
       A.COD_PRESTADOR,
       B.COD_PRINCIPAL_PRESTADOR,
       C.NUM_IDENTIFICACAO_PRINCIPAL_PRESTADOR,
       C.NME_FANTASIA_PRINCIPAL_PRESTADOR,
       C.SGL_UF_PRINCIPAL_PRESTADOR,
       C.NME_MUNICIPIO_PRINCIPAL_PRESTADOR,
       A.NUM_IDENTIFICACAO_PRESTADOR,
       A.NME_FANTASIA_PRESTADOR,
       A.SGL_UF_PRESTADOR,
       A.NME_MUNICIPIO_PRESTADOR,
       B.COD_PRINCIPAL_SERVICO,
       B.DSC_PRINCIPAL_SERVICO,
       C.FLG_ONCOLOGIA,
       C.FLG_INTERNADO_NO_DIA,
       C.FLG_DIAGNOSE_ANALISE_CLINICA,
       C.FLG_DIAGNOSE_IMAGEM,
       C.FLG_DIAGNOSE_OUTRO,
       C.FLG_PROCEDIMENTO,
       C.FLG_TERAPIA_QUIMIOTERAPIA,
       C.FLG_TERAPIA_RADIOTERAPIA,
       C.FLG_MATERIAL,
       C.FLG_MEDICAMENTO,
       C.FLG_PACOTE,
       A.FLG_PS,
       A.FLG_EMERGENCIA,
       A.FLG_INTERNACAO,
       B.COD_PRESTADOR_SOLICITANTE,
       B.NUM_STATUS_SOLICITACAO_VPP,
       B.DSC_STATUS_SOLICITACAO_VPP,
       B.DSC_SITUACAO_QUIMIOTERAPIA_SOLICITACAO_VPP,
       B.DAT_ATENDIMENTO_SOLICITACAO_VPP,
       B.NUM_TIPO_SOLICITACAO_VPP,
       B.COD_TIPO_SOLICITACAO_VPP,
       B.DSC_TIPO_SOLICITACAO_VPP,
       B.DAT_INCLUSAO_SOLICITACAO_VPP,
       B.DAT_INCLUSAO_VPP,
       B.DAT_AUTORIZACAO_VPP,
       B.NUM_SOLICITACAO_VPP,
       B.COD_TIPO_INTERNACAO,
       B.DSC_TIPO_INTERNACAO,
       B.DAT_INTERNACAO,
       B.DAT_ALTA,
       A.DSC_CLASSIFICACAO_SINISTRO,
       A.DSC_TIPO_SINISTRO,
       A.DAT_EXECUCAO_SINISTRO,
       A.DAT_INICIO_COBRANCA_SINISTRO,
       A.DAT_FIM_COBRANCA_SINISTRO,
       A.DAT_PAGAMENTO_SINISTRO,
       A.DAT_MES_PAGAMENTO_SINISTRO,
       A.NUM_VPP,
       A.NUM_INTERNACAO,
       A.COD_SERVICO,
       A.DSC_SERVICO,
       A.DSC_GRUPO_SERVICO,
       A.DSC_SUBGRUPO_SERVICO,
       B.COD_ESPECIALIDADE,
       B.NME_ESPECIALIDADE,
       B.COD_TIPO_PRINCIPAL_SERVICO,
       B.DSC_TIPO_PRINCIPAL_SERVICO,
       B.COD_REGIME_INTERNACAO,
       B.DSC_REGIME_INTERNACAO,
       A.DAT_ATENDIMENTO_EFETIVO_SINISTRO,
       A.SGL_TIPO_GUIA_PRESTADOR,
       A.COD_CID_PRINCIPAL_SINISTRO,
       A.DSC_CID_PRINCIPAL_SINISTRO,
       C.COD_PLANO_MIDTICKET,
       C.NME_PLANO_MIDTICKET,
       B.QTD_VPP_INTERCALADA,
       B.QTD_SR_INTERCALADA,
       B.QTD_INTERNACAO,
       B.QTD_DIAS_INTERNADO,
       B.QTD_PAGO_DIARIA,
       B.QTD_DISPONIVEL_DIARIA,
       B.VLR_PAGO_INTERNACAO_REDE,
       B.VLR_PAGO_INTERNACAO_REEMBOLSO,
       B.VLR_PAGO_INTERNACAO_TOTAL ,
       A.QTD_SERVICO_PAGO,
       A.VLR_APRESENTADO_SERVICO,
       A.VLR_PAGO_SERVICO,
       C.QTD_CONSULTA,
       C.VLR_PAGO_ATENDIMENTO_TOTAL
     
     
     FROM `self-service-saude.TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_ITEM_SERVICO_SINISTRO` A
     LEFT JOIN `self-service-saude.TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_INTERNACAO` B 
     ON  A.COD_CARTEIRINHA_BENEFICIARIO = B.COD_CARTEIRINHA_BENEFICIARIO
     AND A.NUM_INTERNACAO = B.NUM_INTERNACAO
     -- AND B.DAT_INTERNACAO >= '2017-01-01'
     
     LEFT JOIN `self-service-saude.TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_NAO_INTERNACAO` C 
     ON  A.COD_CARTEIRINHA_BENEFICIARIO = C.COD_CARTEIRINHA_BENEFICIARIO
     AND A.DAT_ATENDIMENTO_EFETIVO_SINISTRO = C.DAT_ATENDIMENTO_EFETIVO_SINISTRO
     AND A.COD_SERVICO = C.COD_PRINCIPAL_SERVICO


    
    WHERE DATE_TRUNC(A.DAT_EXECUCAO_SINISTRO , MONTH) BETWEEN '2017-01-01' AND '2020-06-01' 
    AND (DATE_TRUNC(B.DAT_INTERNACAO, MONTH) BETWEEN  '2017-01-01' AND '2020-06-01' 
    OR  B.DAT_INTERNACAO IS NULL)
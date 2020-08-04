 -- TB_EXP_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS

 create or replace table `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOSV1` AS
 
 WITH TB_405 AS (SELECT 
COD_CARTEIRINHA_BENEFICIARIO,
COD_SERVICO,
DE_PARA,
VLR_PAGO_SERVICO,
row_number() over(partition by COD_CARTEIRINHA_BENEFICIARIO, de_para) R_RNK


FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK02_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS` 
WHERE DATE_TRUNC(DAT_EXECUCAO_SINISTRO, MONTH) BETWEEN '2017-01-01' AND '2020-06-01' ),

 TB_BIOPSIA AS
   (
       SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('BIOPSIA') THEN 1 ELSE 0 
                     END) QTD_BIOPSIAS
       FROM TB_405

       GROUP BY 1,2),

--CONTAGEM DE PET CT
TB_PET AS 
(
       SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('PET_CT') THEN 1 ELSE 0 
                     END) QTD_PET
       FROM TB_405
GROUP BY 1,2
),

--QTD QUIMIO E CUSTO

TB_QUIMIO AS 
(

       SELECT
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
    QTD_QUIMIO,
       safe_divide(SUM(VLR_QUIMIO),SUM(QTD_QUIMIO)) MED_QUIMIO
       FROM (

SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('QUIMIOTERAPIA') THEN 1 ELSE 0 
                     END) QTD_QUIMIO,
       SUM(CASE
              WHEN DE_PARA IN ('QUIMIOTERAPIA') THEN VLR_PAGO_SERVICO ELSE 0 
                            END) VLR_QUIMIO
       FROM TB_405
GROUP BY 1,2)
       GROUP BY 1,2,3
),

--qtd radioterapias e custo
TB_RADIO AS 
(

       SELECT
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
    QTD_RADIO,
       safe_divide(SUM(VLR_RADIO),SUM(QTD_RADIO)) MED_RADIO
       FROM (

SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('RADIOTERAPIA') THEN 1 ELSE 0 
                     END) QTD_RADIO,
       SUM(CASE
              WHEN DE_PARA IN ('RADIOTERAPIA') THEN VLR_PAGO_SERVICO ELSE 0 
                            END) VLR_RADIO
       FROM TB_405
GROUP BY 1,2)
       GROUP BY 1,2,3
),

--QTD INTERNACAO ELETIVA E CUSTO

TB_INTER_ELET AS (

       SELECT
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
    QTD_INTER_ELET,
       safe_divide(SUM(VLR_INTER_ELET),SUM(QTD_INTER_ELET)) MED_INTER_ELET
       FROM (

SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('INTERNACAO_ELETIVA') THEN 1 ELSE 0 
                     END) QTD_INTER_ELET,
       SUM(CASE
              WHEN DE_PARA IN ('INTERNACAO_ELETIVA') THEN VLR_PAGO_SERVICO ELSE 0 
                            END) VLR_INTER_ELET
       FROM TB_405
GROUP BY 1,2)
       GROUP BY 1,2,3
),

--QTD_INTERNACAO CLINICA E CUSTO
TB_INTER_CLIN AS
(
       SELECT
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
    QTD_INTER_CLIN,
       safe_divide(SUM(VLR_INTER_CLIN),SUM(QTD_INTER_CLIN)) MED_INTER_CLIN
       FROM (

SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('INTERNACAO_CLINICA') THEN 1 ELSE 0 
                     END) QTD_INTER_CLIN,
       SUM(CASE
              WHEN DE_PARA IN ('INTERNACAO_CLINICA') THEN VLR_PAGO_SERVICO ELSE 0 
                            END) VLR_INTER_CLIN
       FROM TB_405
GROUP BY 1,2)
       GROUP BY 1,2,3
),

--QTD INTERNACAO UTI E CUSTO

TB_INTER_UTI AS
(
       SELECT
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
    QTD_INTER_UTI,
       safe_divide(SUM(VLR_INTER_UTI),SUM(QTD_INTER_UTI)) MED_INTER_UTI
       FROM (

SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('INTERNACAO_UTI') THEN 1 ELSE 0 
                     END) QTD_INTER_UTI,
       SUM(CASE
              WHEN DE_PARA IN ('INTERNACAO_UTI') THEN VLR_PAGO_SERVICO ELSE 0 
                            END) VLR_INTER_UTI
       FROM TB_405
GROUP BY 1,2)
       GROUP BY 1,2,3
),

--PASSAGEM PS E CUSTO

TB_PASSAGEM_PS AS 
(

       SELECT
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
    QTD_PS,
       safe_divide(SUM(VLR_PS),SUM(QTD_PS)) MED_PS
       FROM (

SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('PASSAGEM_PS') THEN 1 ELSE 0 
                     END) QTD_PS,
       SUM(CASE
              WHEN DE_PARA IN ('PASSAGEM_PS') THEN VLR_PAGO_SERVICO ELSE 0 
                            END) VLR_PS
       FROM TB_405
GROUP BY 1,2)
       GROUP BY 1,2,3
),

--internacao cirurgica e custo
TB_INTER_CIRUR AS 
(
       SELECT
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
    QTD_INTER_CIRUR,
       safe_divide(SUM(VLR_INTER_CIRUR),SUM(QTD_INTER_CIRUR)) MED_INTER_CIRUR
       FROM (

SELECT 
       COD_CARTEIRINHA_BENEFICIARIO,
       DE_PARA,
       SUM(CASE 
           WHEN DE_PARA IN ('INTERNACAO_CIRURGICA') THEN 1 ELSE 0 
                     END) QTD_INTER_CIRUR,
       SUM(CASE
              WHEN DE_PARA IN ('INTERNACAO_CIRURGICA') THEN VLR_PAGO_SERVICO ELSE 0 
                            END) VLR_INTER_CIRUR
       FROM TB_405
GROUP BY 1,2)
       GROUP BY 1,2,3),


TB_01 AS (


SELECT distinct 

       A.*,
       B.QTD_BIOPSIAS,
       C.QTD_PET,
       D.QTD_QUIMIO,
       D.MED_QUIMIO,
       E.QTD_RADIO,
       E.MED_RADIO,
       F.QTD_INTER_ELET,
       F.MED_INTER_ELET,
       G.QTD_INTER_CLIN,
       G.MED_INTER_CLIN,
       H.QTD_INTER_UTI,
       H.MED_INTER_UTI,
       I.QTD_PS,
       I.MED_PS,
       J.QTD_INTER_CIRUR,
       J.MED_INTER_CIRUR








 FROM  TB_405 A
       LEFT JOIN TB_BIOPSIA B
       ON  A.COD_CARTEIRINHA_BENEFICIARIO = B.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = B.DE_PARA
       AND A.R_RNK = 1

       LEFT JOIN TB_PET C
       ON  A.COD_CARTEIRINHA_BENEFICIARIO = C.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = C.DE_PARA
       AND A.R_RNK = 1

       LEFT JOIN TB_QUIMIO D
       ON  A.COD_CARTEIRINHA_BENEFICIARIO = D.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = D.DE_PARA
       AND A.R_RNK = 1

       LEFT JOIN TB_RADIO E
    ON  A.COD_CARTEIRINHA_BENEFICIARIO = E.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = E.DE_PARA
       AND A.R_RNK = 1

       LEFT JOIN TB_INTER_ELET F
       ON  A.COD_CARTEIRINHA_BENEFICIARIO = F.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = F.DE_PARA
       AND A.R_RNK = 1

       LEFT JOIN TB_INTER_CLIN G
       ON  A.COD_CARTEIRINHA_BENEFICIARIO = G.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = G.DE_PARA
       AND A.R_RNK = 1

       LEFT JOIN TB_INTER_UTI H
       ON  A.COD_CARTEIRINHA_BENEFICIARIO = H.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = H.DE_PARA
       AND A.R_RNK = 1

       LEFT JOIN TB_PASSAGEM_PS I
       ON  A.COD_CARTEIRINHA_BENEFICIARIO = I.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = I.DE_PARA
       AND A.R_RNK = 1

       LEFT JOIN TB_INTER_CIRUR J
       ON  A.COD_CARTEIRINHA_BENEFICIARIO = J.COD_CARTEIRINHA_BENEFICIARIO
       AND A.DE_PARA = J.DE_PARA
       AND A.R_RNK = 1),


 TB_02 AS ( 
  SELECT 
    A.*,
    B.QTD_BIOPSIAS,
       B.QTD_PET,
       B.QTD_QUIMIO,
       B.MED_QUIMIO,
       B.QTD_RADIO,
       B.MED_RADIO,
       B.QTD_INTER_ELET,
       B.MED_INTER_ELET,
       B.QTD_INTER_CLIN,
       B.MED_INTER_CLIN,
       B.QTD_INTER_UTI,
       B.MED_INTER_UTI,
       B.QTD_PS,
       B.MED_PS,
       B.QTD_INTER_CIRUR,
       B.MED_INTER_CIRUR

  FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK02_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS` A
  LEFT JOIN TB_01 B
  ON  A.COD_CARTEIRINHA_BENEFICIARIO = B.COD_CARTEIRINHA_BENEFICIARIO
  AND A.DE_PARA = B.DE_PARA
  AND B.R_RNK = 1)

SELECT * EXCEPT(DE_PARA) FROM TB_02
WHERE DATE_TRUNC(DAT_EXECUCAO_SINISTRO, MONTH) BETWEEN '2017-01-01' AND '2020-06-01'

create or replace table `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK02_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS_PILOT` as
-- WITH T0 AS(
-- --beneficiarios alto custo
  
--   SELECT DISTINCT
--    A.COD_BENEFICIARIO_CHAVE,
--    B.COD_CARTEIRINHA


--  FROM `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_TOP` A
--  INNER JOIN `self-service-saude.VW_CONS_SAU.TB_DIM_BENEFICIARIO` B
--  ON  A.COD_BENEFICIARIO_CHAVE = CONCAT(B.COD_PREF_EMPRESA,B.COD_EMPRESA,B.COD_FAMILIAR_BENEF,B.COD_RDP)),
 
--  T1 AS (
--  SELECT DISTINCT
--   BENEFICIARIO 

-- FROM `self-service-saude.SBX_ESTRATIFICACAO.TB_MATRIZ_ALGORITMO_IDENTIFICACAO_ONCOLOGIA_ALVOS`
-- WHERE 
-- VERSAO IN ('2.2B2015','2.2B2016','2.2B2017','2.2B2018','2.2B2019') AND 
-- (CANCER_PULMAO IN ('ALTA', 'MEDIA-ALTA') OR 
-- CANCER_MAMA IN ('ALTA', 'MEDIA-ALTA') OR 
-- CANCER_PROSTATA IN ('ALTA', 'MEDIA-ALTA') OR 
-- CANCER_UTERO IN ('ALTA', 'MEDIA-ALTA') OR 
-- CANCER_RETAL IN ('ALTA', 'MEDIA-ALTA') OR 
-- CANCER_LEUCEMIA IN ('ALTA', 'MEDIA-ALTA') OR 
-- CANCER_LINFOMA IN ('ALTA', 'MEDIA-ALTA') OR
-- CANCER_MIELOMA IN ('ALTA', 'MEDIA-ALTA') OR 
-- CANCER_INDETERMINADO IN ('ALTA'))), 


-- T2 AS (
--  -- junção beneficiarios alto custo com estrat
--  SELECT 
 
--  A.COD_CARTEIRINHA
--  FROM T0 A
--  INNER JOIN T1 B
--  ON A.COD_BENEFICIARIO_CHAVE = B.BENEFICIARIO)
-- ,


-- T3 AS (
--   SELECT 

--    A.*

-- FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK01_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS` A
-- INNER JOIN T2 B
-- ON A.COD_CARTEIRINHA_BENEFICIARIO = B.COD_CARTEIRINHA
-- )
--alteração solicitada 14/08/2020

-- select * from T3


--   select count(*) from tb_405, 

--- definição das vpps com os servicos mapeados comentado 14/08/2020
--- definição de serviços com Lucas e Sergio 20/08/2020

  with NUM_VPP2 
  as(
  SELECT DISTINCT
  NUM_INTERNACAO 
  FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK01_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS`
  WHERE COD_SERVICO IN (SELECT 
                COD_SERVICO
                    FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_COD_SERVICOS`))

-- seleção das vpps que contém aqueles servicos mapeados comentado 14/08/2020
-- nova seleção de servicos feita por Lucas e Sergio 20/08/2020


SELECT 

* FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK01_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS`
WHERE NUM_INTERNACAO   IN (SELECT NUM_INTERNACAO   FROM NUM_VPP2)
 

 -- apenas criar a tabela FINAL com nome de pilot caso alguma alteração tenha que ser replicada para lá
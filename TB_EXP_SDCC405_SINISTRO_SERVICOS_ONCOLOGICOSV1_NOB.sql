create or replace table `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS_NOB`  AS 

WITH FILTRO AS (
select DISTINCT COD_ANALITICO_BENEFICIARIO from `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS`
WHERE COD_MOTIVO_EXCLUSAO NOT IN (5,19))

SELECT 
A.* 


FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS` A
INNER JOIN FILTRO B
ON A.COD_ANALITICO_BENEFICIARIO = B.COD_ANALITICO_BENEFICIARIO
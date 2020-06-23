create or replace table `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_EXP_INTER` AS 
WITH T_VIDA AS (SELECT 
COD_CARTEIRINHA_ORIGINAL , 
COD_BENEFICIARIO_CHAVE 
FROM `self-service-saude.VW_CONS_SAU.DM_MGR_VIDA` WHERE DAT_INICIAL = "2020-06-01"

AND
COD_CARTEIRINHA_ORIGINAL IN (
'88888009301040027',
'75183000175850022',
'88888005619040021',
'88888460538710017',
'80446000081860013',
'88888458974110011',
'09003016482210017',
'88888465742020016',
'88888465498260017',
'88888458954670011',
'88888465583280108',
'09003071995700010',
'25593000003540033',
'88888453044070014',
'88888451812880019',
'88888009403190010',
'88888461733390016',
'88888008400550017',
'09003531347550013',
'55123000009080017',
'88888455741240015',
'88888457822580017',
'00444000112090014',
'81702000220340017',
'88888009227810014',
'84469000000440015',
'88888008874870012',
'88888463663280027',
'88888464762130013',
'88888459911950029',
'88888000001400024',
'88888464755550010',
'88888466382960010',
'88888452680790016',
'13250132500010018',
'80446000018660010',
'09003519146460018',
'88888008930290012',
'88888458176180020',
'24160001370540011',
'88888459897050011',
'88888461643140010',
'09001038713840100',
'58963000060630016',
'88888005419930023',
'88888455438860010',
'76382000065610013',
'88888009661850012',
'88888454302710019',
'88888459328560031',
'88888009672490012',
'09001135817470017',
'88888452807890018',
'88888457854200015',
'00373000104451016',
'88888001569090010',
'88888461826910010',
'88888462037970012',
'80446000034040021',
'88888009205090027',
'88888453634310030',
'26267000017760022',
'80449000170100010',
'57396000009260024',
'88888009512110017',
'88888457337440020',
'88888460181400027',
'88888007995720021',
'88888461216400018',
'68513000046990013'))

SELECT
  A.COD_BENEFICIARIO,
  A.COD_BENEFICIARIO_CHAVE,
  B.COD_CARTEIRINHA_ORIGINAL,
  CAST(FORMAT_DATE("%Y%m", DAT_ATENDIMENTO) AS INT64) DAT_REFERENCIA,
  A.COD_PRESTADOR,
  A.NME_FANTASIA_PRESTADOR,
  A.IDC_TIPO_PLANO,
  A.DAT_PAGAMENTO,
  A.DAT_EXECUCAO,
  A.QTD_IDADE,
  A.VAL_SINISTRO,
  A.COD_SERVICO,
  A.DSC_TIPO_PAGAMENTO,
  A.FLG_TIPO_SINISTRO,
  A.FLG_PASSAGEM_PS_PRINCIPAL,
  A.FLG_INTERNACAO_PASSAGEM_PS,
  A.VAL_SINISTRO_PASSAGEM_PS,
  A.DAT_ATENDIMENTO,
  A.DSC_SERVICO,
  A.QTD_SERVICO,
  A.IDT_DOCUMENTO_AP
FROM
  `self-service-saude.VW_CONS_SAU.DM_MGR_SINISTRO` A INNER JOIN T_VIDA B
  ON A.COD_BENEFICIARIO_CHAVE = B.COD_BENEFICIARIO_CHAVE
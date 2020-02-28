TEST

with t1 as(

SELECT * FROM `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_TOP_ABERTO` 
WHERE COD_CARTEIRINHA_ORIGINAL in (
'90307903070020016',
'09411000090700014',
'88888454093380011',
'88888006637250014',
'88888006391090024',
'44980449800040010',
'00562002015710028',
'88888450574410012',
'00444000108180012',
'75182000141250019',
'31813000029120010',
'88888453725520010',
'84311000041620026',
'58963001310540025',
'09041128196450017',
'00295001012190016',
'88888459279270012',
'00530000017580015',
'45812458120020010',
'00680006369040012',
'09003152164130012',
'00444000100500018',
'09003134548620018',
'09035504524350019',
'00680006855640011',
'09003071968600018',
'88888452549370017',
'00680006666170018',
'88888005425640015',
'88888457442110019',
'09003060592290012',
'88888458557970019',
'25505001000030016',
'27805000011160020',
'88888004786540018',
'09003078193310012',
'00552000119240027',
'88888453800410019',
'09037552307840019',
'59268592680010020',
'81575000233830019',
'00679000051890014',
'09010528397900016'))




SELECT 
b.cod_carteirinha_original,
a.COD_BENEFICIARIO_CHAVE, 
a.IDT_DOCUMENTO_AP, 
a.nme_fantasia, 
a.qtd_dias_internacao, 
a.dsc_servico_principal, 
a.dsc_servico_principal_vpp, 
a.dsc_servico_final, 
a.dsc_sexo, 
a.num_idade, 
a.NME_FANTASIA_PRESTADOR_PRINCIPAL, 
a.FLG_REINTERNACAO 
FROM `self-service-saude.SANDBOX_DITER.VW_DM_MGR_INTERNACAO_DITER` a
inner join t1 b 
on  a.COD_BENEFICIARIO_CHAVE = b.COD_BENEFICIARIO_CHAVE
and b.dsc_especialidade in ('ORTOPEDIA')

/*GROUP BY b.cod_carteirinha_original,
a.COD_BENEFICIARIO_CHAVE, 
a.IDT_DOCUMENTO_AP, 
a.nme_fantasia, 
a.qtd_dias_internacao, 
a.dsc_servico_principal, 
a.dsc_servico_principal_vpp, 
a.dsc_servico_final, 
a.dsc_sexo, 
a.num_idade, 
a.NME_FANTASIA_PRESTADOR_PRINCIPAL, 
a.FLG_REINTERNACAO*/

TOMOGRAFIA COMPUTADORIZADA
RESSONÂNCIA MAGNÉTICA

DSC_SERVICO_ABREV NOT IN (

'RESSONANCIA ARTICULAR POR ARTICULACAO',
'TOMOGRAFIA ABD TOT ABDOME SUPER PELVE E RETROPERIT',
'TOMOGRAFIA TORAX',
'TOMOGRAFIA FACE OU SEIOS DA FACE',
'TOMOGRAFIA ABDOMEN SUPERIOR',
'TOMOGRAFIA PELVE OU BACIA',
'RESSONANCIA PELVE NAO INCLUI ARTICUL COXOFEMORAIS',
'RESSONANCIA ABD SUP FIG PANC BACO RIN ADREN RETROP',
'ANGIOTOMOGRAFIA CORONARIANA',
'RESSONANCIA CRANIO ENCEFALO',
'TOMOGRAFIA CRANIO OU SELA TURSICA OU ORBITAS',
'TOMOGRAFIA MANDIBULA',
'TOMOGRAFIA PESC PART MOLES LARINGE TIREO E FARINGE',
'ANGIOTOMOGRAFIA ARTERIAL DE PELVE',
'ANGIOTOMOGRAFIA ARTERIAL DE ABDOME SUPERIOR',
'ANGIO RESSONANCIA ARTERIAL DE CRANIO',	
'ANGIOTOMOGRAFIA ARTERIAL DE TORAX',
'TOMOGRAFIA ARTICULACOES TEMPOROMANDIBULARES',
'ANGIO RESSONANCIA VENOSA DE CRANIO',
'RESSONANCIA MAGNETICA COM OU SEM SEDACAO - PCTE CO',	
'ANGIO RESSONANCIA ARTERIAL DE PESCOCO',	
'ANGIOTOMOGRAFIA DE AORTA TORACICA',
'ANGIOTOMOGRAFIA DE AORTA ABDOMINAL',
'RESSONANCIA PE ANTEPE NAO INCLUI TORNOZELO',	
'ART ESTERNOCLAV OMBR COT PUNHO SACROIL COX JOEL PE',
'HIDRO RESSONANCIA COLANG URO MIELO SIALO CISTOGRAF',	
'RESSONANCIA BACIA ARTICULACOES SACROILIACAS',	
'RM QUALQUER AREA CORPOREA FILME INCLUSO',	
'RESSONANCIA MAMA BILATERAL',
'RESSONANCIA CORACAO MORFOLOGICO E FUNCIONAL',
'RESSONANCIA PESC NASOF OROFAR LARING TRAQ TIREOI',
'RESSONANCIA ARTICULACAO TEMPOROMANDIBULAR BIL',
'RESSONANCIA CORACAO MORF E FUNCIO PERFUSAO ESTRESS',
'RESSONANCIA FLUXO LIQUORICO COMO COMPLEMENTAR',
'RESSONANCIA OSSOS TEMPORAIS BILATERAL',
'TOMOGRAFIA MASTOIDES OU ORELHAS',
'ANGIOTOMOGRAFIA ARTERIAL DE CRANIO',
'ANGIOTOMOGRAFIA ARTERIAL DE PESCOCO')


dsc_servico_principal_vpp 
not in (
	
'CARDIOLOGIA - INTERNACAO CLINICA EM',
'NEFROLOGIA - INTERNACAO CLINICA EM',
'FISIOTERAPIA - CASE',
'UROLOGIA - INTERNACAO EM',
'NEUROLOGIA - INTERNACAO CLINICA EM',
'GERIATRIA E GERONTOLOGIA - INTERNACAO EM',
'ANGIOTOMOGRAFIA ARTERIAL DE TORAX',
'ENDOCRINOLOGIA - INTERNACAO CLINICA EM',
'TESTE DA UREASE (PESQUISA DO HELICOBACTER PYLORI)',
'TRATAMENTO CIRURGICO DE LESAO DO MANGUITO ROTADOR',
'ORTOPEDIA - INTERNACAO EM',
'SUPORTE DE SORO',
'INFECTOLOGIA - INTERNACAO CLINICA EM',
'HEMATOLOGIA - INTERNACAO CLINICA EM',
'ARTROPLASTIA JOELHO C IMPLANTE',
'RETIRADA SHUNT TEMPORARIO',
'TUMOR VESICAL/RESSEC/ENDO/S/HM',
'FRAT FEMUR TRAT CIRURG',
'HEMODINAMICA (PACOTE SEM HONORARIO MEDICO)',
'ARTROPLASTIA TOTAL COXO-FEMURAL COM OU SEM TENOTOM',
'TRAT CIR LUX REC ESC UM P VIA ART OU NAO PCT S HM)',	
'TELEMONITORAMENTO INICIAL   CASOS COMPLEXOS',
'ANGIOLOGIA - INTERNACAO EM',
'TIREOIDECTOMIA TOTAL ( PCTE S/ HM)',
'FISTULA LIQUORICA TRATAMENTO CIRURGICO',
'PONTE SAFENA AORTO-CORONARIA ANASTOMOSE MAMARIO CO',
'INSTALACAO CATETER DE LONGA PERMANENCIA (QQ TIPO)',
'OBESIDADE MORBIDA',	
'HISTEROSCOPIA CIRURGICA PARA BIOPSIA DIRIGIDA  LIS',	
'ARTROPLASTIA DO JOELHO (PCTE SEM HM)',	
'HEPATOLOGIA - INTERNACAO EM',
'CINECORONARIOGRAFIA',
'GASTROENTEROLOGIA - INTERNACAO CLINICA EM',
'TORACOSTOMIA C/DRENAGEM FECHADA',
'ANGIOPLASTIA CORONARIA (COM STENT FARMACOLOGICO) -',
'VISITA DE ENFERMAGEM DOMICILIAR',
'RECONST. DA PAREDE ABDOMINAL C/RET.MUSC.MUOCUTAN.',
'FRATURAS - LUXACOES DA COLUNA: TRATAMENTO CIRURGIC',	
'CIRURGIA DE OBESIDADE MORBIDA POR VIDEO',
'ARTROSCOPIA JOELHO PARA CIRURGIA ASSOC COM TRAT CI',
'ANGIOTOMOGRAFIA CORONARIANA   PCTE COM HM',
'TENOLISE TUNEL OSTEO FIBROSO',
'REUMATOLOGIA - INTERNACAO CLINICA EM',
'TENOPLASTIA OU ENXERTO TENDAO MAO',
'RECONSTRUCAO DE MAMA   PCTE COM HM',
'PNEUMOLOGIA - INTERNACAO CLINICA EM',
'MEDICACAO (EV/IM/SC) 24/24 HS - CASE',	
'MEDICACAO (EV/IM/SC) 12/12 HS- CASE',
'TELEMONITORAMENTO CONTINUIDADE   CASOS COMPLEXOS',	
'PUNCAO VEIA CENTRAL COM COLOCACAO DE CATETER VENOS',
'NEOBEXIGA CUTANEO CONTINENTE',
'RETINOPEXIA C/INTROFLEXAO ESCLERAL+VITRECT+ENDOFOT',
'ONCOLOGIA CLINICA - INTERNACAO EM',
'HISTEROSCOPIA CIRURGICA COM RESSECTOSCOPIA - (PCTE',
'VARIZES - TRATAMENTO CIRURGICO BILATERAL(2 MEMBRO)',
'ANGIOGRAFIA POR CATETER',
'ARTROSCOPIA CIRURGICA EM JOELHO, TORNOZELO, COTOVE',
'COLECISTECTOMIA C/ OU S/ COLANGIOG. VIDEO PCTE S/H',
'HERNIORRAFIA INGUINAL BILATERAL - S/ HM',
'MAMOPLASTIA EM MAMA OPOSTA APOS RECONST P/ MASTECT',
'COLONOSCOPIA',
'RESSONANCIA MAGNETICA - COLUNA CERVICAL',
'BLOQUEIO FENOLICO DE PONTOS MOTORES',
'PSIQUIATRIA - INTERNACAO CLINICA EM',
'DERMOLIPECTOMIA ABDOMINAL NAO ESTETICA',
'CATETERISMO DIR.+ ESQ. + CORONA. + CINEANGIOCARDIO',
'HISTEROSCOPIA CIRURGICA COM BIOPSIASEPTOPLASTIA TURBINECTOMIA',
'PAGAMENTO LIMINAR'
)


DAT_INICIAL
DAT_INTERNACAO
DAT_ALTA
DAT_INCLUSAO_AUDITORIA
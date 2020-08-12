create or replace table `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK01_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS` as 
WITH TB_405_0 AS (
SELECT * FROM `sas-saude-alto-custo-hml.SBX_ALTO_CUSTO.TB_WRK00_SDCC405_SINISTRO_SERVICOS_ONCOLOGICOS` 
)

	SELECT 
	*,
	CASE WHEN COD_SERVICO IN (51050056, 54010381, 30805031, 30805180, 50070134, 30307015, 51050064, 30101077, 48010030, 40403068) THEN 'BIOPSIA'
		 WHEN COD_SERVICO IN (64608506,31080090,40708128) THEN 'PET_CT'
		 WHEN COD_SERVICO IN (43050042, 39130061, 39130010, 96013818, 97008346, 40813916, 32130422, 32131461, 32130031, 40813908, 32131178, 40813924,
							  32800169,96019832,96019840,96019859,96013478,97007420,61490130,61602736,61750735,61433365,61750743,61433357,60018623,
							  61336769,61330035,60015381,61805009,61270253,61341851,61341908,61336750,63080842,63120097,61280305,80031706,72002344,61700118,
							  60023406,61270156,61270202,61280356,61980226,61270300,72070013,97008338) THEN 'QUIMIOTERAPIA'
		 WHEN COD_SERVICO IN (53020138,51039010,52070034,54040183,49010760,31401368,49010875,49010735,
		 	                  30215030,30732018,55030246,52220133,39130134,39130150,31006167,43060080,
		 	                  31006035,54010128,31403107,41020146,30202043,54020182,54020174,41020022,
		 	                  48040045,31403115,54020166,43030289,41020030,46060073,46060065,30302056,
		 	                  30401020,30501180,30502101,30101468,30210127,30212081,54040302,54110173,
		 	                  51040336,50120107,30401038,30312035,41080033,54010438,54010535,41020154,
		 	                  30202051,30101476,54010446,54040264,30210119,51030306,30501199,41080025,
		 	                  51060310,30206065,51060329,54050081,30502098,41090039,41080068,41090020,
		 	                  41080041,50080024,41130030,30308046,30101565,30101573,30101522,54010365,
		 	                  54010578,54010543,41050053,31005080,40202763,49030400,30715202,30715415,
		 	                  31401155,49010751,30302099,56030428,41020057,41020065,30301173,41000013,
		 	                  50130250,41110102,30401089,30211034,31005373,31005381,52070026,30205182,
		 	                  55060307,51040344,30502187,55040047,51050218,30804108,30804191,30212146,
		 	                  30205174,41050142,41050118,30205190,41050126,30205204,41040023,30204100,
		 	                  41040031,30805147,30805260,30205212,43080340,31307124,55060293,41040104,
		 	                  55050220,30601134,45040249,31306063,41050037,55030289,30801060,30801168,
		 	                  41070046,41070062,30501318,51039044,30301190,31307256,41050045,41050061,
		 	                  41040090,41130049,55050085,41040040,41040058,41090047,50120085,41070020,
		 	                  41070038,40202593,40040240,30917042,96007613,96008423,96009128,96008784,
		 	                  96009306,96009322,96009330,96008792,96008806,96003693,96003731,96003677,
		 	                  96003715,96003642,96003685,96006242,96021705,96021713,96021322,96022558,
		 	                  96021330,96022574,96022191,96002760,96012420,96019573,96023139,96014814,
		 	                  96012013,96014261,96010258,96016086,96013133,96018640,96009918,96014245,
		 	                  96009926,96009934,96016094,96016116,96015527,96016124,96015535,96014180,
		 	                  96009942,96012048,96016418,96018658,96015799,96020032,96014849,96015276,
		 	                  96014229,96012064,96012749,96015802,96023155,96013150,96012072,96012757,
		 	                  96015543,96013737,96012080,96014873,96009950,96015586,96015284,96015594,
		 	                  96016159,52070247,31401287,49010697,30906458,31403360,51020386,51020378,
		 	                  31003567,51020092,30402093,51050099,51030217,51050250,30205239,30303109,
		 	                  30203023,53060091,30302137,31203116,53060105,30404134,45090050,47010177,
		 	                  50030060,50130153,50120034,51030225,30501431,30501440,49010549,53060113,
		 	                  30732085,52220117,30732093,52220125,30732107,30732115,52220109,30732123,
		 	                  30732131,99526749,99526756,52220079,52220095,30715385,52010430,52010422,
		 	                  52010414,55020097,31101453,31104185,56050518,31103448,31103456,31103588,
		 	                  53060121,31101461,49010565,54030129,30731232,31401406,49030264,49010530,
		 	                  49010522,49030248,31101470,49030256,52180174,49030426,50130145,97011100,
		 	                  97009032,97009288,97009377,97002291,97015369,97004014,97004103,97004146,
		 	                  97004286,97004421,97015687,97004499,97004529,97004545,97004553,97004561,
		 	                  97004596,97004707,97004790,97008176,96022566,40813029,31005462,31005020,
		 	                  32130791,40813789,40813797,40813819,40813800,32130660,40813045,32130520,
		 	                  40813894,32131160,96003073,20101228,40403033,30011000,40403076,19010249,
		 	                  19010257,40304485,40403815,40403823,40403831,30010993) THEN 'INTERNACAO_CIRURGICA'
		 WHEN COD_SERVICO IN (3160229016020197,97008257,40813037,40813010,32000006,31005640,31005330,
		 	                  60023422,63060043,16010191,40202100,31401112,30803144,96022744,56079010,31201083) THEN 'RADIOTERAPIA'
		 WHEN COD_SERVICO IN (96002581) THEN 'INTERNACAO_CLINICA'
		 WHEN COD_SERVICO IN (61602124,10014985) THEN 'INTERNACAO_ELETIVA'
         WHEN COD_SERVICO IN (61050105,61700592,60001054,61200050,60001070,60001038,60001330,61050300,61040258,
         	                  61050199,61050504,61050253,80017010,61050164,60001046,61800015,61050350,61800058,
         	                  61800040,61700070,60001372,61800023,60001062,60000945,80018009,140,61800066,72000090,
         	                  61000183,61040169,61801054) THEN 'INTERNACAO_UTI'
         WHEN COD_SERVICO IN (60028734,60033550,64620107,10101039,64602974,62010409,66000653,80031501,65621530,
         	                  64150585,62010077) THEN 'PASSAGEM_PS'
		  ELSE NULL END AS DE_PARA
		 FROM TB_405_0
	

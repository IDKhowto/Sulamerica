CREATE OR REPLACE TABLE `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_ORTO` AS (

SELECT * FROM `self-service-saude.VW_CONS_SAU.TB_EXP_CLASSES_SINISTRO_ALL` 
WHERE

--dsc_grupo_servico in ('PROCEDIMENTO', 'MATERIAL', 'DIAGNOSE', 'PACOTE') AND 
DSC_ESPECIALIDADE IN ('ORTOPEDIA', 'NEUROLOGIA','RESSONÂNCIA MAGNÉTICA', 'TOMOGRAFIA COMPUTADORIZADA', '-') AND
dsc_servico_abrev not in (
'MICROCIRURGIA PARA TUMORES INTRACRANIANOS',
'CRANIECTOMIA PARA TUMORES CEREBELARES',
'TRAT CIRURGICO D TUMORES CEREBRAIS SEM MICROSCOPIA',
'CRANIOPLASTIA',
'DERIVACAO VENTRICULAR EXTERNA',
'PUNCAO LIQUORICA',
'OSTEOMIELITE DE FEMUR TRATAMENTO CIRURGICO',
'FIOS PINOS PARAFUSOS OU HASTES METAL INTRA OSSEAS',
'RETIRADA DE FIOS, PARAFUSOS E HASTES INTRA OSSEAS',
'PSEUDARTROSES E OU OSTEOTOMIAS TRAT CIRURGICO',
'IMOBILIZACOES NAO GESSADAS QUALQUER SEGMENTO',
'CONDROPLASTIA COM REMOCAO DE CORPOS LIVRES JOELHO',
'MENISCECTOMIA UM MENISCO',
'OSTEOCONDROPLASTIA ESTABILIZ RESSEC PLASTIA JOELHO',
'LESOES LIGAMENTARES AGUDAS DO TORNOZELO TTO CIRURG',
'OSTEOTOMIAS AO NIVEL DO JOELHO TRAT CIRURGICO',
'TENOPLASTIA ENXERTO DE TENDAO TRAT CIRURGICO',
'PUNCAO ARTICUL DIAG OU TERAP INFILT ORIENT OU NAO',
'RUPTURA DO MANGUITO ROTADOR',
'ACROMIOPLASTIA',
'TUNEL DO CARPO - DESCOMPRESSAO - PROCEDIMENTO VIDE',
'SINOVECTOMIA PARCIAL OU SUBTOTAL JOELHO',
'CONSULTA / ATENDIMENTO PS ORTOPEDIA - PCTE C HM',
'OSTEOTOMIAS E OU PSEUDARTROSES TRAT CIRURGICO',
'MICRONEUROLISE MULTIPLAS',
'SINOVECTOMIA DA MAO MULTIPLAS',
'TENORRAFIA UNICA EM OUTRAS REGIOES',
'RESSECCAO LATERAL DA CLAVICULA',
'SINOVECTOMIA PARCIAL OU SUBTOTAL OMBRO',
'TENOTOMIA DA PORCAO LONGA DO BICEPS',
'TRATAMENTO CIRURGICO DE LESAO DO MANGUITO ROTADOR',
'RETIRADA DE ENXERTO OSSEO',
'REPARO OU SUTURA DE UM MENISCO',
'LESOES LIGAMENTARES PERIFERICAS CRONICAS TRAT CIR',
'MEMBRO SUPERIOR',
'LESAO LIGAMENTAR AGUDA TRATAMENTO CONSERVADOR',
'OSTEOSSINT FRAT FALANGE E METACARP C MINIPARAFUSO',
'LESOES MUSCULO TENDINOSAS TRATAMENTO INCRUENTO',
'BURSECTOMIA TRATAMENTO CIRURGICO',
'BLOQUEIO DE NERVO PERIFERICO',
'RESSECCAO DE NEUROMA',
'INGUINO MALEOLAR',
'TENOPLASTIA DE TENDAO EM OUTRAS REGIOES',
'MICRONEUROLISE UNICA',
'LESA INTRIN JOELH CONDR OSTEOC DISSC ARTROF TT CIR',
'MANIPULACAO ARTICULAR SOB ANESTESIA GERAL',
'PUNCAO BIOPSIA COXO FEMORAL ARTROCENTESE',
'MEMBRO INFERIOR',
'SINOVECTOMIA DE PUNHO TRATAMENTO CIRURGICO',
'FRATURA E OU LUXAGCO DE PUNHO TRAT CONSERVADOR',
'LESOES LIGAMENTARES AGUDAS TRATAMENTO INCRUENTO',
'RECONSTRUCAO, RETENCIONAMENTO OU REFORCO DE LIGAME',
'FRATURA E OU LUXACOES DO ANTEPE REDUCAO INCRUENTA',
'FRATURA DE OSSO DO PE TRATAMENTO CONSERVADOR',
'ENXERTO OSSEO',
'FRATURAS E OU LUXACOES E OU AVULSOES TRAT CIRURG',
'NEUROLISE DAS SINDROMES COMPRESSIVAS',
'TRATAMENTO CIRURGICO DO MAL PERFURANTE PLANTAR',
'ARTROPLASTIA TOTAL JOELHO COM IMPLANTE TTO CIRURG',
'TENOLISE NO TUNEL OSTEO FIBROSO',
'IMPLANTE INTRATECAL D BOMBAS P INFUSAO DE FARMACOS',
'BLOQUEIO DO SISTEMA NERVOSO AUTONOMO',
'SINOVECTOMIA TOTAL OMBRO',
'IMPLANTE DE ELETRODOS CEREBRAL OU MEDULAR',
'MIORRAFIAS',
'ARTROTOMIA COXO FEMORAL TRATAMENTO CIRURGICO',
'ARTROSCOPIA JOELHO PARA CIRURGIA ASSOC COM TRAT CI',
'LESAO AGUDA LIGAMENTO COLAT CRUZADO MENISCO TT CIR',
'INSTAB FEMORO PATELA REL LAT RET REF REC LIGAM MED',
'SINOVECTOMIA TRAT CIRURG TENDOES BURSAS E SINOVIAS',
'TENODESE',
'RESSECCAO PARCIAL OU TOT D CLAVICULA TRAT CIRURG',
'PE TORTO CONGENITO UM PE TRATAMENTO CIRURGICO',
'FRATURA DE JOELHO TRATAMENTO CONSERVADOR',
'FRATURA DE FEMUR TRATAMENTO CONSERVADOR COM GESSO',
'DESBRIDA LABRUM LIGAMEN REDONDO C S CONDROPLASTIA',
'PCTE CONSULTA ATENDIMENTO PS ORTOPEDIA PCTE S HM',
'ARTROSCOPIA DE OMBRO - PCTE DE HM',
'SINOVECTOMIA TOTAL COXO FEMORAL',
'TRATAMENTO DO IMPACTO FEMORO ACETABULAR',
'CONDROPLASTIA COM SUTURA LABRAL',
'OSTEOTOMIA DE COLUNA VERTEBRAL TRAT CIRURGICO',
'TUMORES DE TENDAO OU SINOVIAL TRAT CIRURGICO',
'TENOTOMIA',
'HALLUX VALGUS UM PE TRATAMENTO CIRURGICO',
'ARTROSCOPIA DE OMBRO',
'LESAO LABRAL',
'ARTROSCOPIA P DIAGNOSTICO C OU S BIOPSIA SINOVIAL',
'TRATAMENTO CIRURGICO DA ARTROFIBROSE JOELHO',
'LUXAC CRONICA INVETERADA E RECIDIVAN TRAT CIRURG',
'TUMOR OSSEO RESSECCAO SIMPLES',
'LESOES LIGAMENTARES AGUDAS DO TORNOZELO TTO INCRUE',
'PUNC ARTIC DIAG OU TERAP INFILT SECO ORIENT OU NAO',
'TENOLISE TENDONESE TRATAMENTO CIRURGICO',
'TERCEIRO VENTRICULOSTOMIA',
'LES COMPLEXA JOELHO FRAT LES LIGAMENT MENIS TT CIR',
'ALONGAMENTO/TRANSP OSSEO/PSEUDOARTROSE FIXADOR COX',
'RECONSTR RETENC REFORCO LIGAMENTO CRUZADO ANT POST',
'ARTROSCOPIA DO JOELHO P CIRURG PCT S HM',
'CAPSULECTOMIAS MULTIPLAS MF OU IF',
'RETIRADA DE MATERIAL DE SINTESE TRAT CIRURGICO',
'RESSECCAO D PROCESSO ESTILOIDE D RADIO TRAT CIRURG',
'OSTEOTOMIAS ARTRODESES TRATAMENTO CIRURGICO',
'NEUROLISES DAS SINDR COMPRESSIVAS',
'FRAT REDUCAO ESTABIL SUPERF ARTIC 1 COMPART JOELHO',
'ARTROPLASTIA I - TOTAL DO QUADRIL - PCTE DE HM',
'FRATURAS DE FEMUR TRATAMENTO CIRURGICO',
'PLACAS',
'RETIRADA DE FIXADORES EXTERNOS',
'FRAT TIBIA FIBULA INC DESCOLAMEN EPIFISARIO TT CIR',
'ARTROPLASTIA QQ TECN OU VERSAO DE QUADRIL TRAT CIR',
'ARTROPLASTIA TOTAL COXO FEMURAL COM OU SEM TENOTOM',
'SINOVECTOMIA TOTAL COTOVELO',
'LUVA',
'TENORRAFIA NO TUNEL OSTEOFIBROSO ATE 2 DIGITOS',
'FRATURAS LUXACOES DO TORNOZELO TRAT CIRURGICO',
'NEUROTOMIA SELETIVA DO TRIGEMIO',
'INSTABILIDADE MULTIDIRECIONAL - PROCEDIMENTO VIDEO',
'BIOPSIAS PERCUTANEA SINOVIAL OU DE TECIDOS MOLES',
'TRATAMENTO CIRURGICO DA FISTULA LIQUORICA',
'OSTEOTOMIA PSEUDARTROSE TARSO E MEDIO PE TRAT CIR',
'ARTRODESE METATARSO FALANGICA INTERFALANGI TTO CIR',
'TUMOR OSSEO RESSECCAO E ENXERTO',
'IMPLANTE DE CATETER INTRACRANIANO',
'ARTROPLASTIA ESCAPULO UMERAL IMPLANT TRAT CIRURG',
'SIST DERIVACAO VENTRICULAR INT C VALVULAS OU REVIS',
'ARTRITE SEPTICA TRATAMENTO CIRURGICO JOELHO',
'PACOTE TRIAGEM PROJETO SAUDE ATIVA - PCTE SEM HM',
'REVISOES DE ARTROPLASTIA TOTAL TRAT CIRURGICO',
'PSEUDARTROS OSTEOTOMIA ALONGAM ENCURTAM TTO CIRUR',
'TRAT CIR LUX REC ESC UM P VIA ART OU NAO PCT S HM',
'FRATURA DE UMERO TRATAMENTO CONSERVADOR',
'COLAR',
'FRATURA REDUCAO ESTABILIZ CADA SUPERFICI TORNOZELO',
'PACOTE DE LIQUOR - PCTE COM HM',
'FASCIOTOMIA',
'AMPUTACAO DE DEDO CADA TRATAMENTO CIRURGICO',
'APONEVROSE PALMAR RESSECCAO TRAT CIRURGICO',
'TIPO VELPEAU',
'BIOPSIA CIRURGICA DE JOELHO',
'OSTEOMIELITE DOS OSSOS DA PERNA TRAT CIRURGICO',
'LOCALIZACAO ESTEREOTAXICA C E INTRACRAN C REMOCAO',
'RECONSTRUCAO CRANIANA OU CRANIOFACIAL',
'INST BOMBA INFUSAO ANALGESIA DOR AGUD CRON QQ VIA',
'ARTROSCOPIA JOELHO MENISCECTOMIA',
'TRATAMENTO CIRURGICO DO HEMATOMA INTRACRANIANO',
'BOTA COM OU SEM SALTO',
'FRATUR LUXACA DESCOLAM EPIFISAR COTOV PUNH TTO CIR',
'TRANSPOSICAO DE MAIS DE 1 TENDAO TRAT CIRURGICO',
'ARTROPLASTIA JOELHO COM PROTESE PCTE SEM HM',
'FRATURAS E OU LUXACOES TRATAMENTO CIRURGICO',
'OSTEOTOM COLO TROCANT SUGIOKA MARTIN BOMBEL TT CIR',
'FRATURAS E OU LUXACOES DO PUNHO TRAT CIRURGICO',
'OSTEOTOMIA PSEUDARTROSE METATARSOS FALANGES TT CIR',
'DEFORMIDADE DOS DEDOS TRATAMENTO CIRURGICO',
'CRANIOTOMIA DESCOMPRESSIVA',
'AMPUTACAO AO NIVEL DO PE TRATAMENTO CIRURGICO',
'FRATURA LUXACAO AVULSAO COXO FEMORAL TRAT CIRURG',
'TTO MICROCIR NEUROPATIA COMPRESSIVA TUMORAL INFLAM',
'SINDROME DO CANAL CARPIANO',
'PROTESE D SUBSTITUICAO RETIRADA GRANDES MEDIAS ART',
'FRATURAS LUXACOES NIVEL JOELHO REDUCAO INCRUENTA',
'FRATURAS LUXAC D FALANGE INTERFALANG REDUCAO INCRU',
'FRATURA DE METACARPIANO TRATAMENTO CONSERVADOR',
'FRATURA LUXACAO INCL DESCOLA EPIFISARIO REDUC INCR',
'FIOS OU PINOS METALICOS TRANSOSSEOS',
'REPARACAO LIGAMENTAR DO CARPO',
'AMPUTACAO DESARTICULACAO PODODACTILO P SEG TT CIR',
'BIOPSIA ESTEREOTAXICA DE ENCEFALO',
'ARTROTOMIA TRATAMENTO CIRURGICO JOELHO',
'AMPUTACAO DE PERNA TRATAMENTO CIRURGICO',
'REALINHAMENTOS DO APARELHO EXTENSOR TRAT CIRURG',
'COLETE',
'AXILO PALMAR OU PENDENTE',
'ARTROTOMIA GLENOUMERAL TRATAMENTO CIRURGICO',
'TRANSPOSICAO UNICA DE TENDAO',
'ENXERTO OSSEO PERDA DE SUBSTANCIA TRAT CIRURG',
'OSTEOTOMIA FIXADOR EXTERNO',
'LESOES LIGAMENTARES REDUCAO INCRUENTA',
'PROTESES DE SUBSTITUICAO DE PEQUENAS ARTICULACOES',
'MICRONEUROLISE INTRANEURAL INTRAFASCICULAR 1 NERVO',
'ARTROPLASTIA DE TORNOZELO COM IMPLANTE TRAT CIRURG',
'RETRACAO DE APONEVROSE PALMAR DUPUYTREN',
'SIS SUS 0403020077 NEUROLISE NAO FUNCIONAL NERVOS',
'TOALETE CIRURGICA CORRECAO JOELHO FLEXO TRAT CIRUR',
'ALONGAMENTO DE TENDOES TRATAMENTO CRUENTO',
'ABSCESSOS DE DEDO DRENAGEM TRAT CIRURGICO',
'SINOVECTOMIA TOTAL TORNOZELO',
'FRATURA DA CINTURA PELVICA TRATAMENTO CONSERVADOR',
'TRAT CIRURG FRATURAS DE TIBIA COM FIXADOR EXTERNO',
'FRATURAS E OU LUXACOES REDUCAO INCRUENTA',
'REVISAO ARTROPLASTIA QUADRIL C RET IMPLANTE PROTES',
'ARTROTOMIA TRATAMENTO CIRURGICO PUNHO',
'HIPOFISECTOMIA QQ METODO INCLUI ACESSO NEUROCIRURG',
'TRANSFER MUSCULARES AO NIVEL DO OMBRO TRAT CIRURG',
'FRAT E OU LUXACOES E OU AVULSOES REDUC INCRUENTA',
'DESARTICULACAO COXO FEMORAL TRATAMENTO CIRURGICO',
'PSEUDARTROSES OU OSTEOTOMIAS TRAT CIRURGICO',
'REVISAO CIRURGICA DE PROTESE DE OMBRO',
'FRATURA INCLUI DESCOLAMENTO EPIFISARIO TRAT CIRURG',
'MANGUITO ROTATOR TRAT CIR ASSOC C RESSECCAO S HM',
'MANGUITO ROTATOR TRATAM CIRURGICO',
'TRANSPLANTE OSSEO VASCULARIZADO MICROANASTOMOSE',
'ARTROTOMIA AO NIVEL DA MAO TRATAMENTO CIRURGICO',
'CORPO ESTRANHO INTRA ARTICULAR TRAT CIRURGICO',
'LIBERACAO LATERAL E FACECTOMIAS TRAT CIRURGICO',
'ARTROTOMIA DE TORNOZELO TRATAMENTO CIRURGICO',
'DEDO EM MARTELO TRATAMENTO CIRURGICO',
'DUPLA ABDUCAO OU DUCROQUET',
'DORSO CURVO ESCOLIOSE GIBA COSTAL TRAT CIRURG',
'ABERTURA DE BAINHA TENDINOSA TRAT CIRURGICO',
'ARTRITE OSTEOARTRITE PE INCL OSTEOMIELITE TTO CIR',
'MICRONEUROLISE UNICA PCTE S HM',
'TUMOR OSSEO VERTEB RESSEC SUBSTITUICAO TTO CIR',
'SIH SUS 0408050500 TRATAMENTO CIRURGICO DE FRA',
'SINOVECTOMIA PARCIAL OU SUBTOTAL TORNOZELO',
'OSTEOCONDROPLAST ESTAB RESSEC PLAST ENXER TORNOZEL',
'FRATURA DE OSSO DA MAO TRATAMENTO CONSERVADOR',
'PSEUDOARTROSE PROJETO RELACIONAMENTO PCTE DE HM',
'MICROCIRURGIA VASCULAR INTRACRANIANA',
'LUXACAO GLENO UMERAL',
'LIGAMENTOPLASTIA COM ANCORA',
'FRATURA DE FALANGES TRATAMENTO CONSERVADOR',
'INJ INTRA ART TRAT PUNC ENT CONT DIS MUS',
'INSTR P CIRURG ARTROSCOPICA CUSTO OPERACIONAL',
'BIOPSIA CIRURGICA DE FEMUR',
'RETRACAO CICATR DOS DEDOS C LESAO TENDINEA TTO CIR',
'FRATURA DO ANTEBRACO TRATAMENTO CONSERVADOR',
'TENOARTROPLASTIA PARA OSSOS DO CARPO',
'ARTROPLASTIA DO JOELHO COM IMPLANTE PRIMARIA',
'EXCIS TUMOR NERVOS PERIF C ENXERTO INTERFASCICULAR',
'BIOPSIA DE MUSCULO',
'DISSECCAO MUSCULAR',
'SIH SUS 0413040178 TRATAMENTO CIRURGICO DE LUX',
'TTO NECROS AVASCULAR FORAGEM E NECR FEMORAL TT CIR',
'FRATURA LUXACAO COM FIXADOR EXTERNO TRAT CIRURG',
'EXPLORACAO CIRURGICA DE NERVO NEUROLISE EXTERNA',
'FASCIOTOMIA RESSECCAO FASCIA PLANTAR TTO CIRURGICO',
'ENCURTAMENTO RADIO ULNAR',
'FRATURA E OU LUXACAO DE PATELA TRAT CIRURGICO',
'RECONS RETENC REF LIGA REP CARTILAGEM TRIANG PUNHO',
'ARTRODESE RADIO CARPICA OU DO PUNHO',
'ABSCESSO DE UNHA DRENAGEM TRATAMENTO CIRURGICO',
'ARTRITE SEPTICA TRATAMENTO CIRURGICO',
'TENDINITES SINOVITES E ARTRITES TRAT CIRURGICO',
'LESAO DE NERVOS ASSOCIADA A LESAO OSSEA',
'FRATURA DE COSTELA OU ESTERNO TRAT CONSERVADOR',
'OSTEOMIELITE OSSO S PE TRAT CIRURG',
'RESSECCAO DE OSSO DO PE TRATAMENTO CIRURGICO',
'ARTRODESE INTERFALANG METACARPOFALANG TRAT CIR',
'LESOES LIGAMENT CRONICAS DA MAO REPARACAO CIRURG',
'TTO MICROCIR INTRAMEDUL TU MALFOR AV SIRINGOM PAR',
'REVISAO D REALINHAMENTO APARELHO EXTENSOR TRAT CIR',
'FRAT PSEUDARTR DEFORMID ALONG FIX EXT DINAM TT CIR',
'FRATURA FALANGE OU METACARPIAN TRAT CIR C FIXACAO',
'LESOES LIGAMENTARES AGUDAS DA MAO REPARACAO CIRURG',
'ARTRODIASTASE DE QUADRIL',
'SIH SUS 0408050624 TRATAMENTO CIRURGICO DE FRA',
'ARTROTOMIA DE COTOVELO TRATAMENTO CIRURGICO',
'ARTROPLASTIA COM IMPLANTE TRATAMENTO CIRURGICO',
'TUMOR OSSEO RESSECCAO COM SUBSTITUICAO',
'LESOES AGUDAS LUXACOES MENISCOS 1 OU AMBOS TTO CIR',
'ENXERTOS EM OUTRAS PSEUDARTROSES',
'AMPUTACAO AO NIVEL DA COXA TRATAMENTO CIRURGICO',
'LESOES LIGAMENTARES AGUDAS TRATAMENTO CIRURGICO',
'TENOLISE TUNEL OSTEO FIBROSO',
'SINOVECTOMIA TOTAL PUNHO',
'ROTURAS TENDINO LIGAMENTARES D MAO 1 TTO CIR',
'REVISAO DE SISTEMA IMPLANTADO P INFUSAO D FARMACOS',
'RADICULOTOMIA PCTE SEM HM',
'IMPLANTE DE GERADOR PARA NEUROESTIMULACAO',
'IMPLANTE DE ELETRODO CEREBRAL PROFUNDO',
'COLOCACAO SHUNT DEFINITIVO PCTE SEM HM',
'CORRECAO DEFORMID ADQUIRIDA TIBIA COM FIXADOR EXT',
'FRATURA LUXACOES D PE EXCETO ANTEPE REDU INCRUENTA',
'TENORRAFIA NO TUNEL OSTEOFIBROSO MAIS DE 2 DIGITOS',
'BIOPSIA CIRURGICA DE COTOVELO',
'ARTROPLASTIA INTERFALANG METACARPOFALANG TRAT CIR',
'UNHA ENXERTO TRATAMENTO CIRURGICO',
'FRATURA E OU LUXACOES DO ANTEPE TRAT CIRURGICO',
'PSEUDOARTROSE FEMUR TRAT CIRURG',
'MICROCIR PLEXO BRAQUIAL COM EXPLORACAO E NEUROLISE',
'FRATURAS LUXAC D FALANGE INTERFALANG',
'ARTROPLASTIA JOELHO C IMPLANTE',
'ARTRITE OU OSTEOARTRITE TRATAMENTO CIRURGICO',
'DESINSERCAO OU MIOTOMIA',
'FRATURAS DE FEMUR REDUCAO INCRUENTA',
'LESOES INTRINSECAS DE JOELHO TTO CRUENTO',
'ARTRODESE FIXADOR EXTERNO',
'CORRECAO DEFORMIDADE D PE COM FIX EXT DINAM TT CIR',
'CINGULOTOMIA OU CAPSULOTOMIA UNILATERAL',
'LESOES LIGAMENTARES CRONICAS DO TORNOZELO TTO CIR',
'FRATURA DE COTOVELO TRATAMENTO CONSERVADOR',
'FRATURA D CLAVMCULA OU ESCAPULA TRAT CONSERVADOR',
'DRENAGEM ESTEREOTAXICA CISTO HEMATOMA OU ABSCESSOS',
'ARTROS SIMPLES JOELHO MENISCECTOMIA',
'NEUROLOGIA INTERNACAO CLINICA EM',
'CRURO PODALICO',
'REVISOES D RECONSTRUCOES INTRA ARTICULARES TTO CIR',
'ARTROSC JOELHO LESOES COMPLEXAS',
'IMPLANTACAO DE HALO PARA RADIOCIRURGIA',
'CORPO ESTRANHO INTRA OSSEO TRATAMENTO CIRURGICO',
'TRATAMENTO CIRURGICO DO ABSCESSO ENCEFALICO',
'TREPANACAO PARA PROPEDEUTICA NEUROCIRURGICA',
'FRATURA OU DISJUNCAO DA PELVE TRAT CONSERV S GESSO',
'PSEUDOARTR OSTEOTOMIA CINTURA ESCAPULAR TTO CIRURG',
'FRATURA DE TORNOZELO TRATAMENTO CONSERVADOR',
'FIOS PINOS PARAF HASTES INTRA OSSEOS',
'PCTE EXAME DE LIQUOR HOSP (COLETA E EXAMES) DE HM',
'ENCURTAMENTO DOS OSSOS DA PERNA TRAT CIRURGICO',
'ARTRODESE AO NIVEL DO TORNOZELO TRAT CIRURGICO',
'FRATURA LUXACAO D ESTERNO OU COSTELA REDU INCRUE',
'TRAT CIRURG DA POLIDACTILIA MULTIPLA E OU COMPLEXA',
'SISTEMA NEURONAVEGACAO TX UTILIZ E REGIST S HM',
'SINOVECTOMIA DA MAO 1 ARTICULACAO',
'TRANSPOSICAO DE NERVO',
'AP GESSADO AXILO PALMAR OU PENDENTE',
'ARTROSCOPIA JOELHO P CIRURGIA',
'BIOPSIA CIRURGICA DOS OSSOS DO PE',
'TRATAMENTO CIRURGICO DA OSTEOMIELITE DE CRANIO',
'BIOPSIAS CIRURGICAS DE TENDOES BURSAS E SINOVIAS',
'RECONSTRUCAO DA FALANGE COM RETALHO HOMODIGITAL',
'CIRURGIA INTRACRANIANA POR VIA ENDOSCOPICA',
'ALONGAMENTO',
'TRATAMENTO CIRURGICO FRATURAS COM FIXADOR EXTERNO',
'MICRONEURORRAFIA UNICA',
'ROTURA DO TENDAO DE AQUILES TRATAMENTO CIRURGICO',
'NEUROTOMIA',
'TRANSPOSICAO MUSCULAR',
'TENORRAFIA MULTIPLA EM OUTRAS REGIOES',
'FRATURAS LUXACOES AO NIVEL JOELHO TRAT CIRURGICO',
'FRAT DE FALANGES OU METACARPIANOS RED INCRUENTA',
'FRATURA DE PELVE SEM AP PELVE PODALICO TTO CONSERV',
'CONDROPLASTIA COM REMOCAO DE CORPOS LIVRES PUNHO',
'BRAQUITERAPIA IMPLANTE INTERSTICIAL HDR COM HM',
'ARTROPLASTIA DO JOELHO PCTE SEM HM',
'CONDROPLASTIA COM REMOCAO CORPOS LIVRES TORNOZELO',
'TRANSPLANTES HOMOLOGOS NIVEL DO JOELHO TRAT CIRURG',
'DESCOMPRESSAO VASCULAR DE NERVOS CRANIANOS',
'REVISAO DE SISTEMA DE NEUROESTIMULACAO',
'BIOPSIA DE NERVO',
'MICRONEUROLISE UNICA MICROCIRURGIA',
'DESCOMPRESSAO DE ORBITA OU NERVO OTICO',
'ARTROPLASTIA TOTAL DE JOELHO - PCTE DE HM',
'DRENAGEM VENTRICULAR CONTINUA DIARIA',
'PUNCAO VENTRICULAR COM TREPANACAO',
'OSTEOMIELITE AO NIVEL DA PELVE TRAT CIRURGICO',
'IMOBIL PROV TALAS MEMBRO SUPERIOR',
'TRAT CIR D TUMORES DA REGIAO SELAR ACESSO ENDOSCOP',
'RESSECCAO PARCIAL OU TOTAL CLAVICULA',
'LESAO MANGUITO ROTATOR TRATA CIRURG',
'PE PLANO PE CAVO COALISAO TARSAL TRAT CIRURGICO',
'CRANIOTOMIA EXPLORADORA COM OU SEM BIOPSIA',
'EXCISAO DE TUMORES DOS NERVOS PERIFERICOS',
'ARTROPLASTIA TOTAL',
'OSTEOMIELITE AO NIVEL DA MAO TRAT CIRURGICO',
'MICRONEURORRAFIA DE DEDOS DA MAO',
'TUMORES CEREBRAIS MICROCIRURGIA',
'DERIVACAO VENTRICULO PERITONAL COM SISTEMA VALVULA',
'FRAT PSEUDARTROSES ARTROSES C FIX EXT DINAM TT CIR',
'FISTULA LIQUORICA TRAT CIRURG ENDOSCOP INTRANASAL',
'MENISCORRAFIA TRATAMENTO CIRURGICO',
'BIOPSIA CIRURGICA COXO FEMORAL',
'RESSECCAO DA LESAO COM CIMENTACAO E OSTEOSINTESE',
'BIOPSIA CIRURGICA DE TIBIA OU FIBULA',
'OSTEOMIELITE DE UMERO TRATAMENTO CIRURGICO',
'OSTEOMIELITE AO NIVEL D CINTURA ESCAPULAR TRAT CIR',
'SINOVECTOMIA PARC REMOC CORPOS LIVRES COXO FEMORAL',
'FRATURA DO COCCIX TRATAMENTO CIRURGICO',
'PSEUDARTROSE C PERDA SUBSTANC D METACARP E FALANGE',
'ALONGAMENTO DE FEMUR TRATAMENTO CIRURGICO',
'ALONGAMENTO DOS OSSOS DA PERNA TRAT CIRURGICO',
'OSTEOTOMIA OSSO S PE',
'PSEUDARTROSES TRATAMENTO CIRURGICO',
'FRAT LUXACAO COXO FEMURAL RED INCRUENTA',
'LUXACAO CONG QUADRIL RED INCRUE TENOTOMIA ADUTORES',
'MICRONEURORRAFIA MULTIPLA PLEXO NERVOSO',
'DERIVACAO LOMBAR EXTERNA',
'ARTROPLASTIA QUADRIL INFECT RETIRA COMPONE TTO CIR',
'RETIRADA DE SHUNT DEFINITIVO PCTE SEM HM',
'CORPO ESTRANHO INTRAMUSCULAR TRAT CIRURGICO',
'LOCALIZACAO ESTEREOTAXICA LESOES INTRACRAN C REMOC',
'SIH SUS 0408020342 TRATAMENTO CIRURGICO DE FRA',
'CISTO SINOVIAL TRATAMENTO CIRURGICO',
'ABAIXAMENTO MIOTENDINOSO NO ANTEBRACO',
'MICROCIRURGIA POR VIA TRANSESFENOIDAL',
'CONDROPLASTIA COM REMOCAO D CORPOS LIVRES COTOVELO',
'OSTEOCONDROPLAST ESTAB RESSEC PLAST ENXER COTOVELO',
'PUNCAO BIOPSIA DE COSTELA OU ESTERNO',
'BIOPSIA CIRURGICA DE CINTURA PELVICA',
'ROTURAS APARELHO EXTENSOR D DEDO REDUCAO INCRUENTA',
'FRATURA LUXACAO PATELA INCL OSTEOCONDRAL RED INCRU',
'EXTIRPACAO DE NEUROMA',
'DEDO EM MARTELO TRAT CIRURG',
'FRATU LUXA ANEL PELVICO 1 OU ABORDAGEM TTO CIR',
'FRATURA DE OSSO DO CARPO REDUCAO CIRURGICA',
'DESCOMPRESSAO MEDULAR',
'ALONGAMENTO COM FIXADOR DINAMICO TRAT CIRURGICO',
'TRATAMENTO CIRURGICO DA CRANIOSSINOSTOSE',
'LUXACAO METACARPOFALANGEANA TRATAMENTO CIRURGICO',
'BLOQUEIO NERVO PERIFERICO',
'FRATURA E OU LUXAGCO DE CARPO TRAT CONSERVADOR',
'REPOSICAO DE FARMACO S EM BOMBAS IMPLANTADAS',
'LES ESTEREOTAXICA ESTRUT PROFUN TT DOR OU MOV ANOR',
'RECONSTRUCOES LIGAMENTARES D PIVOT CENTRAL TTO CIR',
'SINOVECTOMIA PARCIAL OU SUBTOTAL COTOVELO',
'CRANIOTOMIA PARA TUMORES OSSEOS',
'ROTURA DO TENDAO DE AQUILES TRATAMENTO INCRUENTO',
'TENOSSINOVITES INFECCIOSAS DRENAGEM',
'FRATURA DE COLUNA COM GESSO TRAT CONSERVADOR',
'MICROCIR PLEXO BRAQUIAL EXPL NEUROLI ENXER INTERFA',
'RETIRADA DE CORPO ESTRANHO TRATAMENTO CIRURGICO',
'ARTRODESE DE JOELHO TRATAMENTO CIRURGICO',
'FRATURA E PSEUDARTROSES FIXADOR EXTER TRAT CIRURG',
'FRATURAS LUXACOES DO TORNOZELO REDUCAO INCRUENTA',
'OSTEOTOM SUPRA ACETABUL CHIARI PEMBERT DIAL TT CIR',
'FIXADOR EXTERNO DINAMICO C S ALONGAMEN TRAT CIRURG',
'SPICA GESSADA',
'ARTROSC JOE TOR COTOV P CIR C RECO LIGA SEM HM',
'CAPSULECTOMIAS UNICA MF E IF',
'ARTROPLASTIAS SEM IMPLANTE TRATAMENTO CIRURGICO',
'LUXACAO CONGENITA QUADRIL REDU CIRUR E OSTEOTOMIA',
'FASCIOTOMIAS DESCOMPRESSIVAS',
'ENXERTO DE NERVO',
'PSEUDARTROSE DO ESCAFOIDE TRATAMENTO CIRURGICO',
'ARTROSCOPIA I - MENISCECTOMIA OU SINOVECTOMIA- PCT',
'SIH SUS 0408050497 TTO CIR FRAT BIMALEOLAR',
'SIH SUS 408050144 RECONSTRUCAO LIGAMENTAR DO T',
'SIH SUS 408060360 RETIRADA DE FIXADOR EXTERNO',
'ALONGAMENTO DO RADIO ULNA TRATAMENTO CIRURGICO',
'TTO CONSERV TCE HIPERTEN INTRACRA HEMORRAGIA P D',
'FRATURA D ACETABULO COM 1 OU ABORDAGEM TRAT CIR',
'FRATURAS E OU LUXACOES DO PUNHO REDUCAO INCRUENTA',
'OSTEOCONDROPLAST ESTAB RESSEC PLAST ENXERTIA PUNHO',
'IMOBILIZ NAO GESSADAS QQUER SEGMENTO',
'ENXERTO INTERFASCICULAR',
'DRENAGEM CIRURGICA DO PSOAS',
'FRATURA E OU LUXACOES D ANEL PELVICO RED INCRUENTA',
'ALONGAMENTO TRANSP OSSEO PSEUDOARTROSE FIXADOR PER',
'PLACA RETIRADA',
'RETIRADA DE CRANIOPLASTIA',
'TRAT CONSERV DO TRAUMATISMO RAQUIMEDULAR POR DIA',
'ARTROPLASTIA DO PUNHO COM IMPLANTE TRAT CIRURG',
'FRATURA DE DOIS OSSOS DA PERNA TRAT CONSERVADOR',
'REVISAO DE ENDOPROTESE',
'CURETAGEM RESSECCAO TUMOR C RECONST E ENXERTO VASC11',
'TRAT CIR LUXACOES ARTRODESE CONTRATURA COM FIX EXT',
'BIOPSIA CIRURGICA DA CINTURA ESCAPULAR',
'AMPUTACAO AO NIVEL DOS METACARPIANOS TRAT CIRURG',
'RETIRADA DE CORPO ESTRANHO CAMARA ANTERIOR PCTE SE',
'TENOSSINOVECTOMIA DE MAO OU PUNHO',
'FRAT TIBIA ASSOC OU NAO FIBULA TRAT CIRURGICO',
'ARTROPLASTIA C IMPLANTE NA MAO MF E IF MULTIPLA',
'DEDO EM GATILHO CAPSULOTOMIA FASCIOTOMIA TRAT CIRU',
'TRACAO CERVICAL TRANSESQUELETICA',
'SIH SUS 408050268 RED INCR LUX FRAT LUX JOELHO',
'ENXERTO PARA REPARO DE 2 OU MAIS NERVOS',
'TRATAMENTO CIRURGICO DA EPILEPSIA',
'ARTROTOMIA QUADRI INFEC INC DREN S RET COMP TT CIR',
'LUXACAO CONGENITA QUADRIL REDUCAO CIRURG SIMPLES',
'REVISAO DE DERIVACAO VENTRIC ATRIAL OU PERITONIAL',
'DERIVACAO VENTRICULAR EXTERNA OU PERITONEAL',
'HALLUX VALGUX UNILAT TRATAMENTO CIRURG PACTE S HM',
'ARTROSCOPIA PARA CIRURGIA OUTRAS ARTICULACOES',
'ALONGAMENTO TENDAO',
'TRACAO TRANSESQUELETICA POR MEMBRO',
'APONEUROSE PLANTAR RESSECC',
'EXPLORACAO CIRURGICA DE TENDAO DE MAO',
'TUMOR OSSEO RESSECCAO SEGMENTAR',
'IMOBIL PROV TALAS MEMBRO INFERIOR',
'OSTEOMIELITE DOS OSSOS DO ANTEBRACO TRAT CIRURG',
'SIH SUS 0408050519 TRATAMENTO CIRURGICO DE FRA',
'ARTROSCOPIA IV - LESOES COMPLEXAS DE JOELHO - PCTE',
'RECONSTRUCAO RETENCIONAMENTO REFORCO DE LIGAMENTO',
'FRATURA DO CARPO REDUCAO INCRUENTA',
'RADICULOTOMIA',
'DESARTICULACAO INTERILIO ABDOMINAL TRAT CIRURGICO',
'FRATURA FIBULA INC DESCOLAMEN EPIFISARIO RED INCRU',
'FIOS PINOS MET TRANSOSSEOS RETIRADA',
'TTO PSEUDO ESCAFOIDE TRANSPL VASC FIX MICROPARAF',
'FRATURA INCL DESCOLA EPIFISARIO REDUC INCRUENTA',
'SINOVECTOMIA TOTAL JOELHO'))
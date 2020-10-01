TRUNCATE TABLE processes;
LOAD DATA LOCAL INFILE 'dados/saida/base01_processos.csv'
INTO TABLE processes
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  comarca,
  foro,
  tribunal,
  uf,
  data_atualizacao,
  vara,
  cpf,
  numero_cnj,
  area,
  arquivado,
  data_distribuicao,
  extinto,
  magistrado,
  numero_alternativo,
  processoID,
  segredo_justica,
  data_sentenca,
  situacao,
  valor,
  numero_unico_trib,
  processo_principal,
  relacao_parte_monitorada,
  polo_parte_monitorada,
  link_processo,
  nome_parte,
  nome_parte_oficial_tse,
  natureza,
  assunto_extra
) SET created_at = NOW();
SHOW WARNINGS;

TRUNCATE TABLE moviments;
LOAD DATA LOCAL INFILE 'dados/saida/base02_movimentacoes.csv'
INTO TABLE moviments
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  numero_cnj,
  processoID,
  data_da_movimentacao,
  tipo_movimentacao,
  texto_da_movimentacao,
  magistrado,
  tribunal,
  instancia,
  numero_unico_trib
)
SET created_at = NOW();
SHOW WARNINGS;

TRUNCATE TABLE parts;
LOAD DATA LOCAL INFILE 'dados/saida/base03_partes.csv'
INTO TABLE parts
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  numero_cnj,
  nome_da_parte,
  cnpj,
  cpf,
  doc_completo_parte,
  autor,
  coautor,
  reu,
  neutro,
  tipo_da_parte,
  doc_completo_parte_limpo,
  numero_unico_trib
)
SET created_at = NOW();
SHOW WARNINGS;

TRUNCATE TABLE politicians;
LOAD DATA LOCAL INFILE 'dados/saida/base04_politicos.csv'
INTO TABLE politicians
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  sg_uf,
  ds_cargo,
  sq_candidato,
  ano_eleicao,
  nm_candidato,
  nm_urna_candidato,
  nr_cpf_candidato,
  sg_partido,
  sg_uf_nascimento,
  nm_municipio_nascimento,
  dt_nascimento,
  ds_genero,
  ds_sit_tot_turno,
  cpf_mascara,
  slug
)
SET created_at = NOW();
SHOW WARNINGS;

TRUNCATE TABLE companies;
LOAD DATA LOCAL INFILE 'dados/saida/base05_politicos_empresas.csv'
INTO TABLE companies
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  cnpj,
  razao_social,
  nome_fantasia,
  NR_CPF_CANDIDATO,
  situacao_cadastral,
  uf,
  municipio,
  nome_socio,
  cpf_socio,
  cnae_fiscal,
  data_atualizacao,
  qualificacao_do_responsavel
);

TRUNCATE TABLE parts_processes;
LOAD DATA LOCAL INFILE 'dados/saida/base06_partes_todas.csv'
INTO TABLE parts_processes
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  numero_unico_trib,
  nome_da_parte,
  cpf,
  parte_tipo,
  polo_politico
)
SET created_at = NOW();
SHOW WARNINGS;

TRUNCATE TABLE base_7;
LOAD DATA LOCAL INFILE 'dados/saida/base07_assuntos.csv'
INTO TABLE base_7
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  natureza,
  assunto_extra,
  numero_unico_trib
)
SET created_at = NOW();
SHOW WARNINGS;

TRUNCATE TABLE politicians_processes_total;
LOAD DATA LOCAL INFILE 'dados/saida/base08_qtdeprocessos.csv'
INTO TABLE politicians_processes_total
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  cpf,
  qt_processos
)
SET created_at = NOW();
SHOW WARNINGS;

TRUNCATE TABLE politicians_processes;
LOAD DATA LOCAL INFILE 'dados/saida/base09_listaprocessos.csv'
INTO TABLE politicians_processes
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES (
  cpf,
  numero_unico_trib
)
SET created_at = NOW();
SHOW WARNINGS;


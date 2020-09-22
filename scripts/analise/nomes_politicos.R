# limpeza nos nomes de políticos
#  descrição: esta rotina transforma os nomes dos políticos nas bases do tse
#  para codificação utf-8, remove todos os acentos e filtra os nomes para
#  apenas nomes que aparecem menos de 5 (cinco) vezes
# andre assumpcao: andre.assumpcao@gmail.com
# reinaldo chaves: reichaves@gmail.com

# importar bibliotecas
library(magrittr)
library(tidyverse)

# carregar dados dos políticos
path1 <- 'dados/entrada/politicos_2016_2018_100maiorescidades_para_producao.xlsx'
candidatos <- readxl::read_excel(path1)

# codificar todos os nomes em UTF-8
Encoding(candidatos$NM_CANDIDATO) <- 'UTF-8'

# transformar os nomes em tipo codificação ascii
candidatos %<>%
  mutate(nome1 = stringi::stri_trans_general(NM_CANDIDATO, 'Latin-ASCII')) %>%
  arrange(desc(ANO_ELEICAO), NR_CPF_CANDIDATO) %>%
  distinct(NR_CPF_CANDIDATO, .keep_all = TRUE)

# contar os nomes iguais
nomes <- candidatos %>% group_by(nome) %>% tally(sort = TRUE)

# salvar planilhas
write_csv(candidatos, path = 'dados/saida/candidatos_info.csv')
write_csv(nomes, path = 'dados/saida/candidatos_nomes.csv')

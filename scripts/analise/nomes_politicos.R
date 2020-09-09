# limpeza nos nomes de políticos
#  descrição: esta rotina transforma os nomes dos políticos nas bases do tse
#  para codificação utf-8, remove todos os acentos e filtra os nomes para
#  apenas nomes que aparecem menos de 5 (cinco) vezes
# andre assumpcao: andre.assumpcao@gmail.com
# reinaldo chaves: reichaves@gmail.com

rm(list = ls())

# importar bibliotecas
library(magrittr)
library(tidyverse)

# carregar dados dos políticos
path1 <- 'dados/entrada/politicos_2016_2018_100maiorescidades_para_producao.xlsx'
# path1 <- 'dados/entrada/candidatos2016.csv'
# path2 <- 'dados/entrada/candidatos2018.csv'
# candidatos <- lapply(
#                 c(path1, path2), read_csv, col_types = cols(.default = 'c')
#               ) %>%
#               lapply(select, -1) %>%
#               bind_rows()
candidatos <- readxl::read_excel(path1)

# # corrigir coluna de processos
# candidatos %<>%
#   mutate(NR_PROCESSO = ifelse(is.na(NR_PROCESSO),NR_PROCESSO_1,NR_PROCESSO))%>%
#   select(-NR_PROCESSO_1)

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

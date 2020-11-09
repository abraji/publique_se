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
path2 <- 'dados/entrada/candidatos_2020_100maiorescidades_para_producao.xlsx'
candidatos01 <- readxl::read_excel(path1)
candidatos02 <- readxl::read_excel(path2)

# filtrat colunas em ambos bancos de dados
candidatos02 %<>%
  select(which(names(candidatos02) %in% names(candidatos01))) %>%
  mutate_all(as.character)

# codificar todos os nomes em UTF-8
Encoding(candidatos01$NM_CANDIDATO) <- 'UTF-8'
Encoding(candidatos02$NM_CANDIDATO) <- 'UTF-8'

# transformar os nomes em tipo codificação ascii
candidatos <- bind_rows(mutate_all(candidatos01, as.character), candidatos02)
candidatos %<>%
  mutate(
    CPF_mascara = ifelse(
      is.na(CPF_mascara),
      paste0('***', str_sub(NR_CPF_CANDIDATO, 4L, 9L), '**'),
      CPF_mascara
    )
  ) %>%
  mutate(nome1 = stringi::stri_trans_general(NM_CANDIDATO, 'Latin-ASCII')) %>%
  arrange(desc(ANO_ELEICAO), NR_CPF_CANDIDATO) %>%
  distinct(NR_CPF_CANDIDATO, .keep_all = TRUE)

# contar os nomes iguais
nomes <- candidatos %>% group_by(nome) %>% tally(sort = TRUE)

# salvar planilhas
write_csv(candidatos, path = 'dados/saida/candidatos_info.csv')
write_csv(nomes, path = 'dados/saida/candidatos_nomes.csv')

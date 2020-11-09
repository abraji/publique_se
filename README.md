# publique_se
Programas utilizados no projeto Publique-se. Os únicos programas excluídos são aqueles que contêm dados de acesso à API do Digesto.

# replicação
Para recriar a análise do Publique-se, você deve ter a distribuição Anaconda instalada e criar o ambiente Python do requirements.yml

```
# crie o ambiente
conda env create -f requirements.yml

# ative o ambiente
conda activate publique-se
```

# observações
O arquivo mestre para esta análise é o arquivo `scripts/produzir_dados.sh`. Esta rotina descreve o que fazemos na análise e acessa os dados que obtivémos junto ao Digesto.

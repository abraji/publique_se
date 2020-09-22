from pathlib import Path
import pandas as pd, numpy as np
import re, os

# definir função principal de execução
def main():

    # paths
    entrada = Path('dados/entrada')
    saida = Path('dados/saida')

    # carregar arquivos de partes, quantidade e contagem
    partes = pd.read_csv(saida / 'base06_partes_todas.csv', dtype=str)
    qtde = pd.read_csv(saida / 'base08_qtdeprocessos.csv', dtype=str)
    contagem = pd.read_csv(saida / 'base09_listaprocessos.csv', dtype=str)

    # carregar correções
    with open(saida / 'eliminar.txt', 'r') as fp:
        nomes = fp.read().split('\n')
        nomes = list(filter(None, nomes))

    # nome=nomes[0]
    # executar loop para eliminar políticos
    for nome in nomes:

        # colunas de interesse do banco de partes
        colunas = ['numero_unico_trib', 'cpf', 'polo_politico']

        # selecionar informação para correção
        correção = partes[partes['nome_da_parte'] == nome][colunas]
        processo = set(correção['numero_unico_trib'].to_list())
        cpf = set(correção[correção['cpf'].notnull()]['cpf'])

        # mudar polo_politico
        partes.loc[partes['nome_da_parte'] == nome, 'polo_politico'] = 'NÃO'
        partes.loc[partes['nome_da_parte'] == nome, 'cpf'] = np.NaN

        # mudar qtde de processos
        qtde = qtde[~qtde['cpf'].isin(cpf)]

        # mudar lista de processos
        contagem = contagem[~contagem['cpf'].isin(cpf)]

    # salvar arquivos de partes, quantidade e contagem
    kwargs = {'quoting':1, 'index': False}
    partes.to_csv(saida / 'base06_partes_todas.csv', **kwargs)
    qtde.to_csv(saida / 'base08_qtdeprocessos.csv', **kwargs)
    contagem.to_csv(saida / 'base09_listaprocessos.csv', **kwargs)

# inserir o bloco de execução principal
if __name__ == '__main__':
    main()

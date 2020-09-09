# importar pacotes
from collections import defaultdict
from pathlib import Path
import argparse
import pandas as pd
import re, os

# definir função para contagem de nomes
def count_words(sample):
    contagem = defaultdict(int)
    for nome in sample:
        contagem[nome] += 1
    return contagem

def extrair_sobrenomes(nome, sobrenomes):
    nome = re.split(' ', nome)
    return list(set(nome) & sobrenomes)

# definir função principal
def main():

    #definir parser na linha de comando
    parser = argparse.ArgumentParser()
    parser.add_argument('--freq', type=int, default=10, nargs=1)
    args = vars(parser.parse_args())

    # definir folder de saida
    saida = Path('dados/saida/')

    # carregar nomes e candidatos
    nomes = pd.read_csv(saida / 'candidatos_nomes.csv')
    candidatos = pd.read_csv(saida / 'candidatos_info.csv')

    # puxar as frequências para os nomes
    candidatos = (
        candidatos
            .drop(columns='nome')
            .merge(nomes, left_on='nome1', right_on='nome')
            .drop(columns='nome1')
    )

    # separar nomes e sobrenomes
    candidatos[['nome', 'sobrenomes']] = (
        candidatos['nome'].str.split(' ', 1, True)
    )

    # criar lista de sobrenomes
    sobrenomes = candidatos['sobrenomes'].to_list()
    sobrenomes = list(map(lambda x: re.split(' ', x), sobrenomes))
    sobrenomes = [subnome for sobrenome in sobrenomes for subnome in sobrenome]

    # contar os sobrenomes para selecionar apenas os menos comuns
    contagem = count_words(sobrenomes)
    contagem = sorted(contagem.items(), key=lambda x: x[1], reverse=True)
    sobrenomes_incomuns = list(filter(lambda x: x[1] < args['freq'], contagem))
    sobrenomes_incomuns = [s[0] for s in sobrenomes_incomuns]
    sobrenomes_incomuns = set(sobrenomes_incomuns)

    # criar uma variável para os sobrenomes parciais e incomuns
    candidatos['sobrenomes_parciais'] = (
        candidatos['sobrenomes'].apply(
            extrair_sobrenomes, args=(sobrenomes_incomuns,)
        )
    )

    # salvar nos dados
    candidatos.to_csv(saida / 'incomuns_nomes.csv', quoting=1, index=False)

# incluir bloco de execução
if __name__ == '__main__':
    main()

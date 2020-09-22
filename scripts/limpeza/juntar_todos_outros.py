#juntar dados dos vários bancos de dados
# autor: andre assumpcao
# andre.assumpcao@gmail.com

#importar pacotes
import json
import multiprocessing as mp
import pandas as pd
import re, os
from pathlib import Path

#definir função para consertar número cnj dos processos
def consertar_cnj(cnj):
    if len(cnj) < 25:
        comarca = cnj[-4:]
        tribunal = f'{cnj[-7]}.{cnj[-6:-4]}'
        ano = cnj[-11:-7]
        cv = cnj[-13:-11]
        numero = cnj[:-13].rjust(7, '0')
        return f'{numero}-{cv}.{ano}.{tribunal}.{comarca}'
    else:
        return cnj

#criar função principal
if __name__ == '__main__':

    #criar objetos comuns
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')
    kwargs = {'index': False, 'quoting': 1}

    #carregar arquivos
    regex = re.compile(r'^(outros|outrosextra)_.*')
    arqvs = list(filter(regex.search, os.listdir(saida)))
    arqvs = sorted([os.path.join(saida, arqv) for arqv in arqvs])

    #separar os arquivos
    regex = lambda pattern: re.compile(pattern)
    detalhes = list(filter(regex('detalhes').search, arqvs))
    partes = list(filter(regex('partes').search, arqvs))
    movs = list(filter(regex('movs').search, arqvs))

    #carregar bancos
    relacionados = pd.read_csv(saida / 'processos_relacionados_outros.csv')
    detalhes = [pd.read_csv(detalhe, low_memory=False) for detalhe in detalhes]
    partes = [pd.read_csv(parte, low_memory=False) for parte in partes]
    movs = [pd.read_csv(mov, low_memory=False) for mov in movs]
    idtribunais = pd.read_csv(entrada / 'numero_unico.csv', dtype=str)

    #concatenar bancos
    detalhes = pd.concat(detalhes)
    partes = pd.concat(partes)
    movs = pd.concat(movs)

    # criar variável de cnj
    cnjs = list(map(consertar_cnj, detalhes['numero'].to_list()))
    detalhes['numero_cnj'] = cnjs

    #apensar cpf e números_cnj nos bancos extras
    match = detalhes[['numero_cnj', 'processoID']].drop_duplicates()

    #puxar os ids digesto para os processos
    partes = partes.merge(match, on='processoID', how='left')
    movs = movs.merge(match, on='processoID', how='left')

    #adicionar indicador de instancia e tribunal de origem
    instancias = detalhes[['processoID', 'instancia', 'tribunal']].copy()
    movs = movs.merge(instancias, on='processoID', how='left')

    #puxar os tribunais e instâncias
    movs = movs.drop_duplicates(movs.columns[1:], ignore_index=True)
    colunas = movs.columns.tolist()
    cnj = colunas.pop(-3)
    movs = movs[[cnj] + colunas]
    movs = movs.astype(str)

    #salvar em arguivo
    detalhes.to_csv(saida / 'processosoutros_detalhes.csv', **kwargs)
    partes.to_csv(saida / 'processosoutros_partes.csv', **kwargs)
    movs.to_csv(saida / 'processosoutros_movs.csv', **kwargs)


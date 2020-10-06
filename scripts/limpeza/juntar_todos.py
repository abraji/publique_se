#juntar dados dos vários bancos de dados
# autor: andre assumpcao
# andre.assumpcao@gmail.com

#importar pacotes
import json
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
def main():

    #criar objetos comuns
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')
    outkwargs = {'index': False, 'quoting': 1}

    #carregar arquivos
    regex = re.compile(r'^(stf|stj|extra)_.*')
    arqvs = list(filter(regex.search, os.listdir(saida)))
    arqvs = sorted([os.path.join(saida, arqv) for arqv in arqvs])

    #separar os arquivos
    regex = lambda pattern: re.compile(pattern)
    detalhes = list(filter(regex('detalhes').search, arqvs))
    partes = list(filter(regex('partes').search, arqvs))
    movs = list(filter(regex('movs').search, arqvs))

    #carregar bancos
    relacionados = pd.read_csv(saida / 'processos_relacionados.csv')
    detalhes = [pd.read_csv(detalhe, low_memory=False) for detalhe in detalhes]
    partes = [pd.read_csv(parte, low_memory=False) for parte in partes]
    movs = [pd.read_csv(mov, low_memory=False) for mov in movs]
    idtribunais = pd.read_csv(entrada / 'numero_unico.csv', dtype=str)

    #apensar cpf e números_cnj nos bancos extras
    match = detalhes[1][['numero_cnj', 'cpf', 'processoID']].copy()

    #puxar os ids digesto para os processos
    detalhes[0] = detalhes[0].merge(match, on='numero_cnj')
    detalhes = pd.concat(detalhes, ignore_index=True)
    detalhes['processoID_x'].fillna(detalhes['processoID'], inplace=True)
    detalhes.drop(columns=['processoID_y', 'processoID'], inplace=True)
    detalhes.rename(columns={'processoID_x': 'processoID'}, inplace=True)
    detalhes['processoID'] = detalhes['processoID'].astype(int)
    detalhes = detalhes.astype(str)
    detalhes['numero_cnj'] = detalhes['numero_cnj'].apply(consertar_cnj)
    detalhes = detalhes.drop_duplicates(ignore_index=True)

    #puxar os ids para partes e movimentações
    match01 = relacionados[['numero_cnj', 'id']].copy()
    match01.rename(columns={'id': 'processoID'}, inplace=True)
    match02 = detalhes[['numero_cnj', 'processoID']].copy()
    match = pd.concat([match01, match02])
    match = match.drop_duplicates('processoID', ignore_index=True)
    match.to_csv(saida / 'processos_lista.csv', **outkwargs)

    #acriar as variáveis necessárias em cada banco
    #partes 0
    partes[0].rename(columns={'numero_cnj': 'processoID'}, inplace=True)
    partes[0] = partes[0].merge(match, on='processoID')
    partes[1] = partes[1].merge(match, on='numero_cnj')
    partes[2] = partes[2].merge(match, on='numero_cnj')
    partes = pd.concat(partes, ignore_index=True)
    partes = partes[[partes.columns[-1]] + list(partes.columns[:-1])]
    partes = partes.astype(str)
    partes['numero_cnj'] = partes['numero_cnj'].apply(consertar_cnj)
    partes = partes.drop_duplicates(ignore_index=True)

    #acertar movimentações
    movs[0].rename(columns={'numero_cnj': 'processoID'}, inplace=True)
    movs[0] = movs[0].merge(match, on='processoID')
    movs[1] = movs[1].merge(match, on='numero_cnj')
    movs[2] = movs[2].merge(match, on='numero_cnj')
    movs = pd.concat(movs, ignore_index=True)
    movs['numero_cnj'] = movs['numero_cnj'].apply(consertar_cnj)

    #criar coluna de tribunal para movimentações
    instancias = relacionados[['id', 'tribunal', 'instancia']].copy()
    instancias.rename(columns={'id': 'processoID'}, inplace=True)
    movs = movs.merge(instancias, on='processoID', how='left')

    # #criar lista de tribunais
    # lista_tribunais = movs[movs['tribunal'].notnull()]['tribunal'].to_list()
    # lista_tribunais = set(lista_tribunais)

    #identificar o tribunal de origem
    movs_comtribunal = movs[movs['tribunal'].notnull()]
    movs_semtribunal = movs[movs['tribunal'].isnull()]
    correção = movs_semtribunal.reset_index()[['index', 'numero_cnj']]
    correção = correção['numero_cnj'].str[16:20].to_frame()
    correção.columns = ['identificador_tribunal']
    substituições = correção.merge(
        idtribunais, on='identificador_tribunal', how='left'
    )['nome'].to_list()

    #mudar os tribunais
    movs_semtribunal.loc[:, 'tribunal'] = substituições

    #juntar as movimentações
    movs = pd.concat([movs_comtribunal, movs_semtribunal])
    movs = movs.drop_duplicates(movs.columns[1:], ignore_index=True)
    colunas = movs.columns.tolist()
    cnj = colunas.pop(-3)
    movs = movs[[cnj] + colunas]
    movs = movs.astype(str)

    #salvar em arguivo
    detalhes.to_csv(saida / 'processos01_detalhes.csv', **outkwargs)
    partes.to_csv(saida / 'processos01_partes.csv', **outkwargs)
    movs.to_csv(saida / 'processos01_movs.csv', **outkwargs)

#criar bloco de execução
if __name__ == '__main__': main()

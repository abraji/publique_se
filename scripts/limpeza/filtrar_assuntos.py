#filtrar os dados mais importantes do digesto
# autor: andre assumpcao
# andre.assumpcao@gmail.com

#importar pacotes
from functools import partial
from itertools import repeat
from pathlib import Path
import ast
import multiprocessing as mp
import os, re
import pandas as pd, numpy as np
import time

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

#definir método para carregar stf
def carregar_processos(saida, kwargs):
    string = r'^processos_(detalhes|partes|movs)'
    regex = re.compile(string)
    arqvs = list(filter(regex.search, os.listdir(saida)))
    arqvs = sorted([os.path.join(saida, arqv) for arqv in arqvs])
    return [pd.read_csv(arqv, **kwargs) for arqv in arqvs]

#definir método para separar assuntos jurídicos
def _split_assunto(textos, ramo_sim):
    texto = re.split(r'\||,(?! )', textos)
    texto = [
        item.upper() if item.upper() in ramo_sim else item for item in texto
    ]
    texto = [item.strip() for item in texto]
    texto = list(filter(None, texto))
    return texto

#definir método para filtrar os assuntos que não sejam nulos
def filtrar_assuntos(df, manutenção):
    return df[df['numero_cnj'].isin(manutenção['numero_cnj'])]

#definir método para confirmar se assunto é de interesse do publique-se
def confirmar_interesse(textos, assuntos, ramo_sim,  ramo_nao):
    sim = len(set(_split_assunto(textos, ramo_sim)) & assuntos) > 0
    nao = len(set(_split_assunto(textos, ramo_sim)) & ramo_nao) == 0
    return sim and nao

#definir função principal
def main():

    #definir objetos comuns à análise
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')
    kwargs = {'quoting': 1, 'low_memory': False, 'dtype': str}

    ##### USAR SE OUTROS NÃO TIVEREM SIDO INCLUÍDOS
    #carregar bancos de processos
    detalhes, movs, partes = carregar_processos(saida, kwargs)

    ##### USAR SE OUTROS JÁ TIVEREM SIDO INCLUÍDOS
    #unpickle bancos
    # detalhes, movs, partes = dados[::3], dados[1::3], dados[2::3]

    # #concatenar
    # detalhes = pd.concat(detalhes)
    # partes = pd.concat(partes)
    # movs = pd.concat(movs)

    #arrumar cpf
    detalhes['cpf'] = detalhes['cpf']\
        .apply(lambda x: str(x) if not pd.isnull(x) else x)\
        .replace(r'\.\d', '', regex=True)\
        .str.pad(11, fillchar='0')

    #arrumar cpf e cnpj em partes
    partes['cnpj'] = partes['cnpj']\
        .apply(lambda x: str(x) if not pd.isnull(x) else x)\
        .replace(r'\.\d', '', regex=True)

    partes['cpf'] = partes['cpf']\
        .apply(lambda x: str(x) if not pd.isnull(x) else x)\
        .replace(r'\.\d', '', regex=True)\
        .str.pad(11, fillchar='0')

    #extrair os índices dos processos cuja coluna "assunto" está vazia
    idx = detalhes[detalhes.assuntoExtra.isnull()].index

    #criar base de dados de descarte de processos e razão do descarte
    descarte = detalhes.iloc[idx, [32, 40]]
    descarte['motivo'] = 'inexistência de assunto'
    descarte.drop_duplicates(inplace=True)
    descarte.to_csv(saida / 'processos_semassunto.csv', index=False)

    #passar descarte nas bases de detalhes, partes e movimentações
    detalhes = detalhes.iloc[~detalhes.index.isin(idx)]
    partes = filtrar_assuntos(partes, detalhes)
    movs = filtrar_assuntos(movs, detalhes)

    #importar tabela processual da união
    tpu = pd.read_excel(entrada / 'identificadores_tpu_avaliado.xlsx')

    #definir conjuntos dos ramos do direito e assuntos jurídicos
    ramo_sim = set(tpu.loc[tpu['INTERESSA'] == 'SIM', 'nivel1'])
    ramo_nao = set(tpu.loc[tpu['INTERESSA'] == 'NÃO', 'nivel1']) - ramo_sim
    assuntos = set(tpu.loc[tpu['INTERESSA'] == 'SIM', 'ultimo_assunto'])
    args = (assuntos, ramo_sim, ramo_nao)

    #filtrar apenas os processos de interesse público
    assuntos = detalhes['assuntoExtra'].apply(confirmar_interesse, args=args)
    detalhes.loc[:, 'status_publiquese'] = 2
    detalhes['status_publiquese'].where(assuntos, 1, inplace=True)

    #apagar processos duplicados com mais de um cpf
    filtro = ['numero_cnj', 'cpf']
    detalhes = detalhes.drop_duplicates(filtro, ignore_index=True)

    #criar lista de processos com assuntos relevantes
    cnj = detalhes[detalhes['status_publiquese'] == 2]['numero_cnj']
    cnj = set(cnj.to_list())

    #filtrar estes processos nos outros bancos
    detalhes = detalhes[detalhes['status_publiquese'] == 2]
    partes = partes[partes['numero_cnj'].isin(cnj)]
    movs = movs[movs['numero_cnj'].isin(cnj)]

    #converter tipo
    detalhes = detalhes.astype(str)
    partes = partes.astype(str)
    movs = movs.astype(str)

    #eliminar partes com nome nulo
    partes = partes[partes['nome_normalizado'].notnull()]

    #salvar em disco
    kwargs = {'index': False, 'quoting': 1, 'float_format': '{:,.0f}'}
    detalhes.to_csv(entrada / 'politicos_detalhes.csv', **kwargs)
    partes.to_csv(entrada / 'politicos_partes.csv', **kwargs)
    movs.to_csv(saida / 'politicos_movs.csv', **kwargs)

# inserir bloco de execução principal
if __name__ == '__main__':
    main()

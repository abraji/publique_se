import os, re, shutil
import pandas as pd
import zipfile
from csv import QUOTE_ALL
from tqdm import tqdm

# função para descomprimir arquivos zip
def descomprimir_dados(origem=None, destino='dados/saida/'):

    """essa função descomprime os dados dos candidatos do site do tse"""

    # verificar se existe origem para arquivos. caso contrário, definir
    if not origem: origem = 'dados/entrada/'

    # formar path para arquivo(s)
    if os.path.isfile(origem):
        arqvs = [os.path.join(origem)]
    else:
        regex = re.compile(r'consulta_cand_.*\.zip$')
        arqvs = list(filter(regex.search, os.listdir(origem)))
        arqvs = [os.path.join(origem, arqv) for arqv in arqvs]

    # achar ano dos arquivos
    origem = os.path.abspath(origem)
    anos = [str(ano[-8:-4]) for ano in arqvs]

    # executar descompressão
    for ano, arqv in tqdm(zip(anos, arqvs)):
        os.makedirs(os.path.join(destino, f'eleicao{str(ano)}'), exist_ok=True)
        with zipfile.ZipFile(arqv, 'r') as z:
            z.extractall(os.path.join(destino, f'eleicao{str(ano)}'))

# criar função para juntar os dados por eleição
def juntar_dados(pasta):

    """ essa função junta os arquivos txt de uma eleição """

    # definir parâmetros de limpeza de dados
    juntar_dados.kwargs = {
        'header': 0, 'index_col': False, 'encoding': 'latin1',
        'engine': 'python', 'quotechar': '"', 'quoting': QUOTE_ALL,
        'doublequote': False, 'dtype': str
    }

    regex = re.compile(r'(BR|BRASIL)\.csv$')
    arqvs = list(filter(regex.search, os.listdir(pasta)))
    arqvs = [os.path.join(pasta, arqv) for arqv in arqvs]
    candidatos = [pd.read_csv(arqv, ';', **juntar_dados.kwargs) for arqv in arqvs]
    return pd.concat(candidatos)

# criar wrapper para processar as duas funções acima conjuntamente
def limpar_dados(pastas, origem=None, destino='dados/saida/'):
    descomprimir_dados(origem, destino)
    return [juntar_dados(pasta) for pasta in pastas]

# executar função se módulo principal
if __name__ == '__main__':

    # executar função principal
    dados = limpar_dados([f'dados/saida/eleicao{ano}' for ano in [2016, 2018]])

    # salvar em disco
    kwargs = {'index': False, 'quoting': QUOTE_ALL}
    dados[0].to_csv('dados/entrada/candidatos2016.csv', **kwargs)
    dados[1].to_csv('dados/entrada/candidatos2018.csv', **kwargs)

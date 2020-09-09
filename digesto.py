# conexão com api do digesto
# andre assumpcao: andre.assumpcao@gmail.com
# reinaldo chaves: reichaves@gmail.com

# import libraries
import json, re, requests
import pandas as pd

# definir classe
class Digesto:

    """ essa classe contém as funções para busca na api do digesto """

    # definir método init
    def __init__(self, key):

        # armazenar key
        self.key = key

        # define parâmetros de autenticaçõa
        self.endpoint = 'https://op.digesto.com.br/api/'
        self.headers  = {'Authorization': 'Bearer ' + key}

        # define parâmetros de cada buscar
        self.processo = self.endpoint + 'tribproc/'
        self.processo_parte = self.endpoint + 'tribprocs/buscar?&'
        self.processo_resumo = self.endpoint + 'tribproc/search_parte_procs'
        self.processo_tabela = self.endpoint + 'tribproc/render_proc_as_report'
        self.tribunalIDs = self.endpoint + 'tribproc_tribunal?per_page=500'
        self.assuntos = self.endpoint + 'tribproc/assuntos'

    # search lawsuits by individual identifier (cnj)
    def baixar_processos(self, cnj, params=None, tabela=False):
        is_tabela = isinstance(cnj, list)
        if is_tabela:
            r = requests.post(
                self.processo_tabela, json={'cnjs': cnj}, headers=self.headers
            )
        else:
            r = requests.get(
                self.processo + cnj, params=params, headers=self.headers
            )
        if r:
            return r.json()

    # search lawsuits by plaintiff or defendant's name
    def baixar_parte(self, params):
        params = '&'.join([f'{k}={v}' for k, v in params.items()])
        params = re.sub(r'\'', '\"', params)
        r = requests.get(self.processo_parte + params, headers=self.headers)
        try:
            if isinstance(r, requests.models.Response):
                return r.json()
        except:
            pass

    # search summary of all lawsuits for one person
    def baixar_resumos(self, payload):
        r = requests.post(
            self.processo_resumo, json=payload, headers=self.headers
        )
        if r:
            return r.json()

    # get list of court IDs
    def baixar_tribunais(self):
        r = requests.get(self.tribunalIDs, headers=self.headers)
        r = r.json()
        columns = ['$uri', 'nome', 'sigla']
        r = [{k: v for k, v in t.items() if k in columns} for t in r]
        return r

    # get list of legal issues:
    def baixar_assuntos(self):
        r = requests.get(self.assuntos, headers=self.headers)
        r = r.json()
        colunas = [f'id{i},nivel{i}' for i in range(1, 6)]
        colunas = [coluna for nivel in colunas for coluna in nivel.split(',')]
        nivel1, nivel2, nivel3, nivel4, nivel5 = [], [], [], [], []
        for n1 in r:
            nivel1 += [(n1['id'], n1['name'])]
            for n2 in n1['assuntos']:
                nivel2 += [(n1['id'], n1['name'], n2['id'], n2['name'])]
                for n3 in n2['assuntos']:
                    nivel3 += [(
                        n1['id'], n1['name'], n2['id'], n2['name'], n3['id'],
                        n3['name']
                    )]
                    for n4 in n3['assuntos']:
                        nivel4 += [(
                            n1['id'], n1['name'], n2['id'], n2['name'],
                            n3['id'], n3['name'], n4['id'], n4['name']
                        )]
                        for n5 in n4['assuntos']:
                            if n4['assuntos']:
                                nivel5 += [(
                                    n1['id'], n1['name'], n2['id'], n2['name'],
                                    n3['id'], n3['name'], n4['id'], n4['name'],
                                    n5['id'], n5['name']
                                )]
        return pd.DataFrame(
            nivel1+nivel2+nivel3+nivel4+nivel5, columns=colunas
        )

import json
import re
import requests


class Digesto:

    """classe para obtenção dos processos via api Digesto"""

    def __init__(self, key):

        """load key and endpoints"""

        # define key de acesso
        self.key = key

        # define parâmetros de autenticaçõa
        self.endpoint = "https://op.digesto.com.br/api/"
        self.headers = {"Authorization": "Bearer " + key}

        # define parâmetros de cada buscar
        self.teste_api = self.endpoint + "user/current"
        self.processo = self.endpoint + "tribproc/"
        self.processo_parte = self.endpoint + "tribproc/search_parte_procs"
        self.tribunalIDs = self.endpoint + "tribproc_tribunal?per_page=500"
        self.assuntos = self.endpoint + "tribproc/assuntos"

    def testar_api(self):
        """check whether api is working"""
        r = requests.get(self.teste_api, headers=self.headers)
        return r

    def buscar_processos_parte(self, nome_parte):
        """search lawsuits by plaintiff or defendant name"""
        r = requests.post(
            url=self.processo_parte,
            json={'nome_parte': nome_parte},
            headers=self.headers
        )
        return r

    def baixar_processo(self, processoID, params=None):
        """download known lawsuit"""
        processo_url = f'{self.processo}{processoID}'
        r = requests.get(processo_url, params=params, headers=self.headers)
        return r

    def atualizar_processo(self, processoID, params={}):
        """update known lawsuit"""
        params.update({
            'id_update_callback': 'abc123',
            'atualiza_tribunal_anexos': True
        })
        processo_url = f'{self.processo}{processoID}'
        r = requests.get(processo_url, params=params, headers=self.headers)
        return r




    # def baixar_processos(self, cnj, params=None, tabela=False):

    #     """search lawsuits by individual identifier (cnj)"""

    #     isinstance(cnj, list)


    #     is_tabela = isinstance(cnj, list)
    #     if is_tabela:
    #         r = requests.post(
    #             self.processo_tabela, json={"cnjs": cnj}, headers=self.headers
    #         )
    #     else:
    #         r = requests.get(
    #             self.processo + cnj, params=params, headers=self.headers
    #         )
    #     if r:
    #         return r.json()

    # def baixar_resumos(self, payload):

    #     """ search summary of all lawsuits for one person"""

    #     r = requests.post(
    #         self.processo_resumo, json=payload, headers=self.headers
    #     )
    #     if r:
    #         return r.json()

    # def baixar_tribunais(self):

    #     """ get list of court IDs"""

    #     r = requests.get(self.tribunalIDs, headers=self.headers)
    #     r = r.json()
    #     columns = ["$uri", "nome", "sigla"]
    #     r = [{k: v for k, v in t.items() if k in columns} for t in r]
    #     return r

    # def baixar_assuntos(self):

    #     """ get list of legal issues:"""

    #     r = requests.get(self.assuntos, headers=self.headers)
    #     r = r.json()
    #     colunas = [f"id{i},nivel{i}" for i in range(1, 6)]
    #     colunas = [coluna for nivel in colunas for coluna in nivel.split(",")]
    #     nivel1, nivel2, nivel3, nivel4, nivel5 = [], [], [], [], []
    #     for n1 in r:
    #         nivel1 += [(n1["id"], n1["name"])]
    #         for n2 in n1["assuntos"]:
    #             nivel2 += [(n1["id"], n1["name"], n2["id"], n2["name"])]
    #             for n3 in n2["assuntos"]:
    #                 nivel3 += [
    #                     (
    #                         n1["id"],
    #                         n1["name"],
    #                         n2["id"],
    #                         n2["name"],
    #                         n3["id"],
    #                         n3["name"],
    #                     )
    #                 ]
    #                 for n4 in n3["assuntos"]:
    #                     nivel4 += [
    #                         (
    #                             n1["id"],
    #                             n1["name"],
    #                             n2["id"],
    #                             n2["name"],
    #                             n3["id"],
    #                             n3["name"],
    #                             n4["id"],
    #                             n4["name"],
    #                         )
    #                     ]
    #                     for n5 in n4["assuntos"]:
    #                         if n4["assuntos"]:
    #                             nivel5 += [
    #                                 (
    #                                     n1["id"],
    #                                     n1["name"],
    #                                     n2["id"],
    #                                     n2["name"],
    #                                     n3["id"],
    #                                     n3["name"],
    #                                     n4["id"],
    #                                     n4["name"],
    #                                     n5["id"],
    #                                     n5["name"],
    #                                 )
    #                             ]
    #     return pd.DataFrame(
    #         nivel1 + nivel2 + nivel3 + nivel4 + nivel5, columns=colunas
    #     )


# class JusBrasil(Digesto):

#     """classe para obtenção dos processos via api Dossier Jusbrasil"""

#     def __init__(self, key):

#         # define key de acesso
#         self.key = key

#         # define parâmetros de autenticaçõa
#         self.endpoint = "https://dossier-api.jusbrasil.com.br"
#         self.headers = {"Authorization": key}

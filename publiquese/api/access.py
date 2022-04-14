import json
import re
import requests


class Digesto:

    """Digesto API class"""

    def __init__(self, key):

        """load key and endpoints"""

        # define key de acesso
        self.key = key

        # define parâmetros de autenticação
        self.endpoint = "https://op.digesto.com.br/api/"
        self.headers = {"Authorization": "Bearer " + key}

        # define parâmetros de cada buscar
        self.teste_api = self.endpoint + "user/current"
        self.processo = self.endpoint + "tribproc/"
        self.processo_parte = self.endpoint + "tribproc/search_parte_procs"

    def testar_api(self):

        """check whether API is working"""

        r = requests.get(self.teste_api, headers=self.headers)
        return r

    def buscar_processos_parte(self, nome_parte):

        """search lawsuits by plaintiff or defendant name"""

        r = requests.post(
            url=self.processo_parte,
            json={"nome_parte": nome_parte},
            headers=self.headers,
        )
        return r

    def baixar_processo(self, id_processo, params=None):

        """download known lawsuit"""

        params = {"tipo_numero": 8} if not params else params
        processo_url = f"{self.processo}{id_processo}"
        r = requests.get(processo_url, params=params, headers=self.headers)
        return r

    def atualizar_processo(self, id_processo, params=None):

        """update known lawsuit"""

        if not params:
            params = {"atualiza_tribunal_anexos": "true", "tipo_numero": 8}

        processo_url = f"{self.processo}{id_processo}"
        r = requests.get(processo_url, params=params, headers=self.headers)
        return r


class Jusbrasil:

    """Classe para obtenção dos processos via API Dossier Jusbrasil"""

    def __init__(self, key):

        # define key de acesso
        self.key = key

        # define parâmetros de autenticaçõa
        self.endpoint = "https://dossier-api.jusbrasil.com.br/v5/"
        self.endpoint_dossier = self.endpoint + "dossier/"
        self.endpoint_lawsuit = self.endpoint + "lawsuit/"
        self.headers = {"Authorization": key}

    def consultar_processo(self, id_processo):
        pass

    def criar_dossier(self, params):
        pass

    def listar_dossiers(self):
        pass

    def executar_dossier(self, dossier_id):
        pass

    def listar_arquivos_dossier(self, dossier_id, offset, kind="lawsuits"):
        pass

    def download_arquivos_dossier(self, dossier_id):
        pass

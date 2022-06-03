import json
import re
import requests

from requests.models import Response
from typing import Optional


class Digesto:

    """Digesto API class"""

    def __init__(self, key: str) -> None:

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

        # define parâmetros principais

    def testar_api(self) -> Response:

        """check whether API is working"""

        r = requests.get(self.teste_api, headers=self.headers)
        return r

    def buscar_processos_parte(self, nome_parte: str) -> Response:

        """search lawsuits by plaintiff or defendant name"""

        r = requests.post(
            url=self.processo_parte,
            json={"nome_parte": nome_parte},
            headers=self.headers,
        )
        return r

    def baixar_processo(
        self, id_processo: str, params: dict = None
    ) -> Response:

        """download known lawsuit"""

        params = {"tipo_numero": 8} if not params else params
        processo_url = f"{self.processo}{id_processo}"
        r = requests.get(processo_url, params=params, headers=self.headers)
        return r

    def atualizar_processo(
        self, id_processo: str, params: dict = None
    ) -> Response:

        """update known lawsuit"""

        if not params:
            params = {"atualiza_tribunal_anexos": "true", "tipo_numero": 8}

        processo_url = f"{self.processo}{id_processo}"
        r = requests.get(processo_url, params=params, headers=self.headers)
        return r

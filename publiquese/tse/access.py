import requests
from pathlib import Path

from tqdm import tqdm


class TSE:

    """TSE data download clsss"""

    def __init__(self, data_folder=None):

        # define main url
        self.url = "https://cdn.tse.jus.br/estatistica/sead/odsele/"

        # define endpoints
        self.candidatos = self.url + "consulta_cand/"
        self.resultados = self.url + "votacao_candidato_munzona/"

        # instantiante path variable to store datasets
        if not data_folder:
            self.data_folder = Path().resolve() / "data-raw"

        # create data folder if it doesn't exist yet
        self.data_folder.mkdir(parents=True, exist_ok=True)

    def testar_api(self):

        """check whether API is working"""

        r = requests.get("https://dadosabertos.tse.jus.br/group/candidatos")
        return r

    def baixar_candidatos(self, years):

        """download electoral results"""

        if not isinstance(years, list):
            years = [years]

        for year in tqdm(years):
            file = f"consulta_cand_{year}.zip"
            r = requests.get(self.candidatos + file, stream=True)
            with open(self.data_folder / file, "wb") as fp:
                for chunk in r.iter_content(chunk_size=128):
                    fp.write(chunk)

            return f"Arquivo Baixado: {file}"

    def baixar_resultados(self, years):

        """download candidates"""

        if not isinstance(years, list):
            years = [years]

        for year in tqdm(years):
            file = f"votacao_candidato_munzona_{year}.zip"
            r = requests.get(self.resultados + file, stream=True)
            with open(self.data_folder / file, "wb") as fp:
                for chunk in r.iter_content(chunk_size=128):
                    fp.write(chunk)

            return f"Arquivo Baixado: {file}"

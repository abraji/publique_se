import concurrent.futures
import os, re, sys
import requests
import time
from pathlib import Path
from datetime import datetime

# load custom modules
sys.path.append(os.path.abspath(os.path.join(".")))
import publiquese

# ROOT = Path("tests").resolve()
# DATA = ROOT / "data"

with open('tests/api_key.txt', 'r') as fp:
  key = fp.readline()
  key = key.replace('\n', '')

with open("tests/api_token.txt", "r") as fp:
    token = fp.readline()
    token = token.replace("\n", "")


# Jusbrasil = Jusbrasil(token)
# Jusbrasil.endpoint_dossier

# iniciar classes
Digesto = publiquese.Digesto(key)
# Jusbrasil = publiquese.Jusbrasil(token)

# DIGESTO: baixar processo de Fernando Holiday vs. Ciro Gomes usando o
# identificador interno do digesto e o identificador CNJ
r01 = Digesto.baixar_processo("1021887-19.2020.8.26.0100")
r02 = Digesto.baixar_processo("399037238")
r01.json() == r02.json()

resposta = r01.json()
data_update = max(resposta['acessos'])
data_update = datetime.strptime(data_update, '%Y-%m-%d %H:%M:%S')

diff = datetime.today() - data_update
diff.days <= 3

hoje = datetime.strftime(datetime.today(), '%d/%m/%Y')

# JUSBRASIL
params = {
    "filter": ["Fernando Holiday Silva Bispo"],
    "kind": "LAWSUIT",
    "artifacts": ["lawsuits"],
}

header = {'Authorization': token}
r = requests.post(Jusbrasil.endpoint_lawsuit, data={'cnj_number': "1021887-19.2020.8.26.0100"}, headers=header)
r.status_code


url = 'https://dadosabertos.tse.jus.br/group'
# define main url
url = 'https://cdn.tse.jus.br/estatistica/sead/odsele/'

# define endpoints
candidatos = url + "consulta_cand/"
data_folder = Path().resolve() / 'data-raw'
year = [2018][0]
from tqdm import tqdm
for year in tqdm(years):
    file = f'consulta_cand_{year}.zip'
    r = requests.get(candidatos + file, stream=True)
    with open(data_folder / file, 'wb') as fp:
        for chunk in r.iter_content(chunk_size=128):
            fp.write(chunk)

import os, sys
import json
import time
from pathlib import Path
from csv import reader
from datetime import datetime

from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(".")))
import publiquese


def converter_cnj(c):
    return f'{c[:7]}-{c[7:9]}.{c[9:13]}.{c[13:14]}.{c[14:16]}.{c[16:]}'


def main():

    # definir caminhos para os folders de interesse
    ROOT = Path().resolve()
    DATA = ROOT / 'data'
    DATA_RAW = ROOT / 'data-raw'

    # carregar a key de acesso à API do Digesto
    with open(ROOT / 'tests' / 'api_key.txt', 'r') as fp:
        key = fp.readline()
        key = key.replace('\n', '')

    # carregar os processos da fase 01 do publique-se que foram incluídos
    # no ano de 2020
    processos = DATA_RAW / 'base09_listaprocessos.csv'
    with open(processos, 'r') as file:
        processos = reader(file)
        processos = [tuple(processo) for processo in processos]

    # extrair apenas os números cnj e os salvar em lista única
    processos_unicos = list(set([processo[1] for processo in processos]))

    # converter para formato certo
    numeros_cnj = [converter_cnj(p) for p in processos_unicos]
    numeros_cnj = sorted(numeros_cnj)

    # instanciar classe do digesto
    Digesto = publiquese.Digesto(key)

    # atualizar processos. adicionar sleep para não fazer
    dados = []
    for numero_cnj in tqdm(numeros_cnj):
        try:
            r = Digesto.baixar_processo(numero_cnj)
            if r.json():
                dados.append(r.json())
        except:
            pass

    # how many were updated
    dates = [max(dado['acessos']) for dado in dados]
    updates = [re.search(r'^(202[12])', date) for date in dates]
    update_count = len([update for update in updates if update])
    print(f'{update_count:,}: Número de processos atualizados.')

    # add timestamp to download
    today = datetime.today().strftime('%Y-%m-%d')
    file = DATA_RAW / f'processos_{today}.json'

    # save files
    with open(file, 'w') as fp:
        json.dump(dados, fp)


if __name__ == '__main__':
    main()

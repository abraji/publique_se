# importar pacotes
from pathlib import Path
import argparse
import re, os

import pandas as pd

# define main function
def main():

    # definir path
    ROOT = Path()
    ENTRADA = Path('dados/entrada/')
    SAIDA = Path('dados/saida/')
    CHECAGEM = ROOT / 'dados/saida/checagens'

    # definir argumentos para leitura dos dados
    kwargs = {
        'index_col': False, 'quoting': 1, 'low_memory': False, 'dtype': str
    }

    # definir parser na linha de comando
    descrição = 'Defina quais tipos de processos devem ser filtrados.'
    parser = argparse.ArgumentParser(description=descrição)
    parser.add_argument('--status_publiquese', type=int, default=2, nargs='+')
    parser.add_argument('--tamanho', type=int, default=100, nargs='?')
    args = vars(parser.parse_args())

    # importar dados
    detalhes = pd.read_csv(ENTRADA / 'politicos_detalhesFiltrado.csv',**kwargs)
    partes = pd.read_csv(ENTRADA / 'politicos_partesFiltrado.csv', **kwargs)

    # eliminar duplicados em partes
    partes.drop_duplicates(['numero_cnj', 'nome_normalizado'], inplace=True)

    # criar os file paths dos arquivos de checagem
    fp = []
    for root, _, arqvs in os.walk(SAIDA / 'checagens'):
        planilhas = []
        for arqv in arqvs:
            if not re.search(r'DS_Store|participantes\.csv', arqv):
                planilha = os.path.join(root, arqv)
                fp.extend([planilha])

    # filtrar e selecionar só os arquivos de partes
    fps = sorted(list(filter(lambda x: re.search(r'_partes', x), fp)))
    fps = list(map(Path, fps))
    last_folder = int(os.path.split(fps[-1])[0][-3:])

    # carregar os dados
    checagem = pd.concat([pd.read_excel(fp, dtype=str) for fp in fps])
    checagem = checagem.reset_index(drop=True)

    # isolar apenas os processos cujas partes não tenham sido checadas
    já_checados = set(checagem['numero_cnj'])
    detalhes = detalhes[~detalhes['numero_cnj'].isin(já_checados)]
    partes = partes[~partes['numero_cnj'].isin(já_checados)]

    # recuperar argumento da linha de comando
    if isinstance(args['status_publiquese'], int):
        filtro = [args['status_publiquese']]
    else:
        filtro = args['status_publiquese']

    # selecionar o banco com base no filtro de status
    detalhes['status_publiquese'] = (
        detalhes['status_publiquese'].str.replace('.0', '').astype(int)
    )
    detalhes = detalhes[detalhes['status_publiquese'].isin(filtro)]

    # importar participantes do desafio
    participantes = pd.read_csv(SAIDA / 'checagens/01_participantes.csv')

    # definir arquivos de folders para merge futuro
    folders = []

    # definir loop para criar processos
    for participante in participantes.itertuples(name=None):

        # criar número do folder e seu filepath
        idx = f'{last_folder+participante[0]+1:03d}'
        folder = CHECAGEM / f'lote{idx}'

        # filtrar as planilhas
        sorteados_detalhes = detalhes.sample(args['tamanho'])
        sorteados_partes = (
            partes[partes['numero_cnj'].isin(sorteados_detalhes['numero_cnj'])]
        )

        # armazenar número do folder e filepath para criar as planilhas
        folders.append((participante[0], folder))

        # criar folder e path para planilhas
        os.mkdir(folder)
        path_partes = folder / f'lote{idx}_partes.xlsx'
        path_detalhes = folder / f'lote{idx}_detalhes.xlsx'

        # selecionar as colunas dos bancos
        cols_detalhes, cols_partes = [
            'numeroAlternativo', 'juiz', 'area', 'assuntoExtra', 'comarca',
            'tribunal', 'distribuicaoData', 'segredo_justica', 'arquivado',
            'classes', 'numero_cnj'
        ], [
            'numero_cnj', 'nome_normalizado', 'cnpj', 'cpf',
            'relacao_normalizado'
        ]

        # salvar planilhas
        sorteados_partes[cols_partes].to_excel(path_partes, index=False)
        sorteados_detalhes[cols_detalhes].to_excel(path_detalhes,index=False)

        # filtrar sorteados
        detalhes = detalhes.loc[~detalhes.index.isin(sorteados_detalhes.index)]
        partes = partes.loc[~partes.index.isin(sorteados_partes.index),]

    # apensar folder paths no banco de participantes
    folders = pd.DataFrame(columns=['idx', 'folder'], data=folders)
    folders = folders.set_index('idx')

    # juntar os folders
    participantes = (
        participantes.merge(folders, left_index=True, right_index=True)
    )

    # salvar em disco
    kwargs = {'index': False, 'quoting': 1}
    participantes.to_csv(CHECAGEM / '02_participantes.csv', **kwargs)

# inserir bloco de execução principal
if __name__ == '__main__':
    main()

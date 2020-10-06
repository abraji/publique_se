#importar pacotes
from pathlib import Path
import argparse
import pandas as pd
import re, os

#definir função principal
def main():

    #definir parser na linha de comando
    parser = argparse.ArgumentParser(
        description='Defina quais tipos de processos devem ser filtrados.'
    )
    parser.add_argument('--status_publiquese', type=int, default=2, nargs='+')
    parser.add_argument('--checador', type=str, default='reinaldo', nargs='+')
    parser.add_argument('--tamanho', type=int, default=100, nargs=1)
    args = vars(parser.parse_args())

    #definir objetos comuns à análise
    kwargs = {
        'index_col': False, 'quoting': 1, 'low_memory': False, 'dtype': str
    }
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')

    #importar dados
    detalhes = pd.read_csv(entrada / 'politicos_detalhesFiltrado.csv', **kwargs)
    partes = pd.read_csv(entrada / 'politicos_partesFiltrado.csv', **kwargs)

    #eliminar duplicados em partes
    partes.drop_duplicates(['numero_cnj', 'nome_normalizado'], inplace=True)

    #carregar os arquivos de checagens
    arqvs = [arqvs for _, _, arqvs in os.walk(saida / 'checagens')]
    arqvs = sorted(arqvs)
    arqvs = [arqv for path in arqvs for arqv in path]
    regex = re.compile(r'_partes_')
    arqvs = list(filter(regex.search, arqvs))
    regex = re.compile(r'(\d{3})(_partes)')
    arqvs = [
        str(saida / f'checagens/lote{re.search(regex, path).group(1)}' / path)
        for i, path in enumerate(arqvs)
    ]
    arqvs = [Path(arqv) for arqv in arqvs]

    #carregar os dados
    checagem = pd.concat(
        [pd.read_excel(arqv, dtype=str) for arqv in arqvs], ignore_index=True
    )

    #isolar apenas os processos cujas partes não tenham sido checadas
    já_checados = set(checagem['numero_cnj'])
    detalhes = detalhes[~detalhes['numero_cnj'].isin(já_checados)]
    partes = partes[~partes['numero_cnj'].isin(já_checados)]
    # args = {'status_publiquese': [3, 4, 5]}
    #recuperar argumento da linha de comando
    if isinstance(args['status_publiquese'], int):
        filtro = [args['status_publiquese']]
    else:
        filtro = args['status_publiquese']

    #selecionar o banco com base no filtro de status
    detalhes['status_publiquese'] = (
        detalhes['status_publiquese'].str.replace('.0', '').astype(int)
    )
    detalhes = detalhes[detalhes['status_publiquese'].isin(filtro)]

    #selecionar os processos por amostragem
    detalhes = detalhes.sample(args['tamanho'])
    partes = partes[partes['numero_cnj'].isin(detalhes['numero_cnj'])]

    #definir onde começar o próximo lote
    lote_atual = re.search(r'(lote)(\d{3})', str(arqvs[-1])).group(2)
    lote_prox = f'{int(lote_atual)+1:03}'

    #criar o novo folder
    try:
        os.mkdir(saida / f'checagens/lote{lote_prox}')
    except:
        print(f'o folder "{saida / f"checagens/lote{lote_prox}"}" já existe.')

    #criar o path para os arquivos de checagem
    partes_path = re.sub(lote_atual, lote_prox, str(arqvs[-1]))
    detalhes_path = re.sub(r'partes', 'detalhes', partes_path)

    #criar os arquivos
    detalhes.to_excel(Path(detalhes_path), index=False)
    partes.to_excel(Path(partes_path), index=False)

# definir bloco de execução
if __name__ == '__main__':
    main()

#script para juntar dados do publiquese
from pathlib import Path
import argparse
import pandas as pd
import re, os

#executar if main
if __name__ == '__main__':

    #criar objetos comuns
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')

    #criar argparser da shell
    parser = argparse.ArgumentParser()
    parser.add_argument('--tribunais', required=True, nargs='*')
    args = vars(parser.parse_args())

    #executar o loop
    for tipo in args['tribunais']:

        #criar path para arquivos
        arqvs = os.listdir(saida)
        detalhes_arqvs, partes_arqvs, movs_arqvs = \
            [re.search(f'{tipo}' + r'_detalhes.*', arqv) for arqv in arqvs], \
            [re.search(f'{tipo}' + r'_partes.*', arqv) for arqv in arqvs], \
            [re.search(f'{tipo}' + r'_movs.*', arqv) for arqv in arqvs]

        #eliminar none
        detalhes = list(filter(None, detalhes_arqvs))
        partes = list(filter(None, partes_arqvs))
        movs = list(filter(None, movs_arqvs))

        #criar bancos de dados
        detalhes = pd.concat([
            pd.read_csv(saida / f'{detalhe.group(0)}', dtype=str)
                for detalhe in detalhes
            ],
            ignore_index=True
        )

        partes = pd.concat([
            pd.read_csv(saida / f'{parte.group(0)}', dtype=str)
                for parte in partes
            ],
            ignore_index=True
        )

        movs = pd.concat([
            pd.read_csv(saida / f'{mov.group(0)}', dtype=str)
                for mov in movs
            ],
            ignore_index=True
        )

        #salvar em arquivo
        detalhes.to_csv(saida / f'{tipo}_detalhes.csv', index=False, quoting=1)
        partes.to_csv(saida / f'{tipo}_partes.csv', index=False, quoting=1)
        movs.to_csv(saida / f'{tipo}_movs.csv', index=False, quoting=1)

        #filtrar filepath nulos
        detalhes_arqvs = list(filter(None, detalhes_arqvs))
        partes_arqvs = list(filter(None, partes_arqvs))
        movs_arqvs = list(filter(None, movs_arqvs))

        #pegar filepath inteiro
        detalhes_arqvs = list(map(lambda x: x.group(0), detalhes_arqvs))
        partes_arqvs = list(map(lambda x: x.group(0), partes_arqvs))
        movs_arqvs = list(map(lambda x: x.group(0), movs_arqvs))

        #apagar os arquivos duplicados
        list(map(os.remove, [saida / file for file in detalhes_arqvs]))
        list(map(os.remove, [saida / file for file in partes_arqvs]))
        list(map(os.remove, [saida / file for file in movs_arqvs]))

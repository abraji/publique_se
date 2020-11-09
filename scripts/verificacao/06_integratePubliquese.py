#carregar a consolidação manual de partes
# autor: andre assumpcao
# andre.assumpcao@gmail.com

#importar pacotes
from pathlib import Path
import pandas as pd, numpy as np
import re, os

#definir método principal
def main():

    #definir objetos comuns à análise
    kwargs = {
        'index_col': False, 'quoting': 1, 'low_memory': False, 'dtype': str
    }
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')

    #carregar o arquivo de partes
    detalhes = pd.read_csv(entrada/'politicos_detalhesFiltrado.csv', **kwargs)
    partes = pd.read_csv(entrada / 'politicos_partesFiltrado.csv', **kwargs)

    #carregar base de políticos
    politicos2016 = pd.read_excel(
        entrada / 'politicos_2016_2018_100maiorescidades_para_producao.xlsx',
        converters={'NR_CPF_CANDIDATO': lambda x: str(x).rjust(11)}
    )
    politicos2020 = pd.read_excel(
        entrada / 'candidatos_2020_100maiorescidades_para_producao.xlsx',
        converters={'NR_CPF_CANDIDATO': lambda x: str(x).rjust(11)}
    )

    politicos2016['NR_CPF_CANDIDATO'] = (
        politicos2016['NR_CPF_CANDIDATO']
            .apply(lambda x: str(x) if not pd.isnull(x) else x)
            .replace(r' *', '', regex=True)
            .str.pad(11, fillchar='0')
    )
    politicos2020['NR_CPF_CANDIDATO'] = (
        politicos2020['NR_CPF_CANDIDATO']
            .apply(lambda x: str(x) if not pd.isnull(x) else x)
            .replace(r' *', '', regex=True)
            .str.pad(11, fillchar='0')
    )

    politicos = politicos2016.NR_CPF_CANDIDATO.to_list()
    politicos.extend(politicos2020.NR_CPF_CANDIDATO.to_list())
    cpfs = set(politicos)

    #carregar os arquivos de checagens
    arqvs = [arqvs for _, _, arqvs in os.walk(saida / 'checagens_final')]
    arqvs = sorted(arqvs)
    arqvs = [arqv for path in arqvs for arqv in path]
    regex = re.compile(r'_partes_')
    arqvs = list(filter(regex.search, arqvs))
    regex = re.compile(r'(\d{3})(_partes)')
    arqvs = [
        str(saida/f'checagens_final/lote{re.search(regex, path).group(1)}'/path)
        for i, path in enumerate(arqvs)
    ]
    arqvs = sorted([Path(arqv) for arqv in arqvs])

    #carregar os dados
    check = pd.concat(
        [pd.read_excel(arqv, dtype=str) for arqv in arqvs], ignore_index=True
    )

    #arrumar os dados
    check[['id1', 'processoID']] = check[['id1', 'processoID']].apply(
        lambda x: x.str.replace(r'\.0', '')
    )
    check = check[check['numero_cnj'].notnull()].drop_duplicates()

    #arrumar cpf
    check['cpf'] = check['cpf']\
        .apply(lambda x: str(x) if not pd.isnull(x) else x)\
        .replace(r'\.\d', '', regex=True)\
        .str.pad(11, fillchar='0')

    #criar método de substituição de cpfs usando número cnj e nome
    # find = check.dropna(subset=['cpf'])
    find = check[['numero_cnj', 'nome_normalizado', 'cpf']]
    find = find.drop_duplicates().reset_index(drop=True)

    #usar indexação de linhas para esta substituição
    partes = partes.reset_index()
    substr = partes.merge(find, on=['numero_cnj', 'nome_normalizado'])
    substr = substr.drop_duplicates('index')

    #fazer substituição de cpfs no banco de partes apenas para os cpfs
    # que só foram adicionados com a checagem manual. se estes cpfs
    # tiverem sido encontrados de outra forma, continuaremos a indicar
    # esta outra forma.
    substr = substr.drop(columns=['cpf_x']).rename(columns={'cpf_y': 'cpf'})

    #definir condições para achar os lugares para substituição
    substituir_cpf = substr['cpf'].isin(cpfs)
    substituir_02 = substr['status_politico'].eq('2')
    substituir_04 = substr['status_politico'].eq('4')

    #subtituir linhas
    substr.loc[(substituir_cpf & (substituir_02 | substituir_04)),
        'status_politico'
    ] = '6'

    # reordenar as linhas para apagá-las posteriormente
    substr = substr.sort_values(
        ['numero_cnj', 'nome_normalizado', 'status_politico']
    )

    # substr.groupby('status_politico').count()

    #jogar fora e substituir
    partes = partes.drop(substr['index'].tolist(), axis=0)
    partes = pd.concat([partes, substr])
    partes = partes.sort_values('index').drop(columns={'index'})

    #pares políticos e processos
    pares = (
        partes
            .sort_values(['numero_cnj', 'nome_normalizado', 'status_politico'])
            .drop_duplicates(['numero_cnj', 'nome_normalizado'])
    )

    #definir processos com informações de políticos
    status02 = pares[['numero_cnj', 'status_politico']]

    #juntar numero_cnj e processos
    status01 = detalhes.merge(status02, on='numero_cnj')

    #jogar fora variáveis, reordenar colunas, etc.
    status01 = status01.drop(columns={'status_publiquese'})
    status01 = status01.rename(columns={'status_politico':'status_publiquese'})
    status01 = status01[
        status01.columns[:-2].tolist() + ['status_publiquese', 'cpf']
    ]
    status01 = status01.drop_duplicates(status01.columns[:-1])

    #imprimir resultado
    resultados = status01.groupby('status_publiquese').count()
    resultados = resultados[['numero_cnj', 'cpf']]

    for status in resultados.itertuples(name=None):
        print(
            f'\n Status: {int(status[0])} \n -----------\n',
            f'Nº processos: {status[1]:>5}\n',
            f'Nº políticos: {status[2]:>5}\n\n'
        )

    #salvar os arquivos em disco
    status01.to_csv(saida / 'politicos_detalhes.csv', index=False, quoting=1)
    partes.to_csv(saida / 'politicos_partes.csv', index=False, quoting=1)

# inserir bloco de execução
if __name__ == '__main__':
    main()

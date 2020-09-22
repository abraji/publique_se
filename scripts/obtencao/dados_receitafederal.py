#script para baixar dados da receita federal
#andre assumpcao: andre.assumpcao@gmail.com
#reinaldo chaves: reichaves@gmail.com

#importar módulos
from csv import QUOTE_ALL
from pathlib import Path
from tqdm import tqdm
from unicodedata import normalize
import os, requests, pandas as pd, zipfile

# definir função para apagar acentos de nomes de políticos
def utf8_ascii(var):
    return normalize('NFKD', var).encode('ascii', 'ignore').decode('utf-8')

#criar função para organizar base05
def organizar_base05():

    #definir argumentos para carregar csv bem pesado de sócios
    kwargs_ = {
        'header': 0, 'dtype': str, 'encoding': 'utf-8', 'decimal': ',',
        'usecols': [
            'cnpj', 'nome_socio', 'cnpj_cpf_socio',
            'percentual_capital_socio', 'cod_qualificacao_socio',
            'cpf_representante_legal', 'nome_representante',
            'NR_CPF_CANDIDATO'
        ]
    }

    #carregar base de sócios de empresas (26 milhões de linhas)
    rf_socios = pd.read_csv(saida / 'rf_socios.csv', **kwargs_)

    #definir argumentos para carregar csv bem pesado de empresas
    kwargs_ = {
        'header': 0, 'dtype': str, 'encoding': 'utf-8', 'decimal': ',',
        'usecols': [
            'cnpj', 'identificador_matriz_filial', 'razao_social',
            'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
            'motivo_situacao_cadastral', 'nm_cidade_exterior',
            'codigo_natureza_juridica', 'data_inicio_atividade', 'cnae_fiscal',
            'uf', 'municipio', 'qualificacao_responsavel',
            'capital_social_empresa'
        ]
    }

    #carregar base de empresas (41 milhões de linhas)
    rf_empresas = pd.read_csv(saida / 'rf_empresas.csv', **kwargs_)

    #juntar empresas e sócios
    rf_empresas = rf_empresas.merge(rf_socios, how='inner', on='cnpj')
    rf_empresas.rename(
        columns={'qualificacao_responsavel': 'qualificacao_do_responsavel'},
        inplace=True
    )

    #selecionar apenas colunas mais importantes
    rf_empresas['data_atualizacao'] = '04/07/2020'
    rf_empresas['cpf_socio'] = \
        [f'***{row}**' for row in rf_empresas['NR_CPF_CANDIDATO'].str[3:9]]
    rf_empresas = rf_empresas[[
        'cnpj', 'razao_social', 'nome_fantasia', 'NR_CPF_CANDIDATO',
        'situacao_cadastral', 'uf', 'municipio', 'nome_socio', 'cpf_socio',
        'cnae_fiscal', 'data_atualizacao', 'qualificacao_do_responsavel',
    ]]

    #converter rótulos das situações das empresas
    categorias = {
        '01': 'nula', '02': 'ativa', '03': 'suspensa', '04': 'inapta',
        '08': 'baixada'
    }
    sit = [categorias[i] for i in rf_empresas['situacao_cadastral'].to_list()]
    rf_empresas['situacao_cadastral'] = sit

    #converter rótulos das qualificações dos sócios
    qualificacao = {
        '05': 'administrador',
        '10': 'diretor',
        '11': 'interventor',
        '12': 'inventariante',
        '13': 'liquidante',
        '16': 'presidente',
        '17': 'procurador',
        '18': 'secretário',
        '19': 'síndico (condomínio)',
        '22': 'sócio',
        '23': 'sócio capitalista',
        '24': 'sócio comanditado',
        '28': 'sócio-gerente',
        '31': 'sócio ostensivo',
        '32': 'tabelião',
        '33': 'tesoureiro',
        '34': 'titular de empresa individual imobiliária',
        '36': '',
        '39': 'diplomata',
        '40': 'cônsul',
        '41': 'representante de organização internacional',
        '42': 'oficial de registro',
        '43': 'responsável',
        '46': 'ministro de Estado das relações exteriores',
        '49': 'sócio-administrador',
        '50': 'empresário',
        '51': 'candidato a cargo político eletivo',
        '54': 'fundador',
        '59': 'produtor rural',
        '60': 'cônsul honorário',
        '61': 'responsável indígena',
        '62': 'representante da instituição extraterritorial',
        '64': 'administrador judicial',
        '65': 'titular pessoa física residente ou domiciliado no Brasil',
        '77': 'vice-presidente'
    }
    rf_empresas['qualificacao_do_responsavel'] = [
        qualificacao[i]
        for i in rf_empresas.qualificacao_do_responsavel.to_list()
    ]
    rf_empresas['NR_CPF_CANDIDATO'] = rf_empresas['NR_CPF_CANDIDATO']\
        .astype(str).str.pad(11, fillchar='0')

    #criar path de destino
    fname = saida / 'base05_politicos_empresas.csv'

    #criar coluna no banco de dados, resetar o índice e salvar em csv
    rf_empresas.drop_duplicates(inplace=False)
    rf_empresas.reset_index(drop=True, inplace=True)
    rf_empresas.to_csv(fname, quoting=QUOTE_ALL, index=False)

# definir bloco de execução
if __name__ == '__main__':

    #paths
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')

    #baixar arquivos do github
    url = 'https://bit.ly/2XjlVa3'
    chunksize = 1000000
    r = requests.get(url, stream=True)
    with open(entrada / 'cnpj.zip', 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunksize):
            fd.write(chunk)

    #descomprimir arquivos
    with zipfile.ZipFile(entrada / 'cnpj.zip', 'r') as zfile:
        zfile.extractall(entrada / 'cnpj/')

    #listar os arquivos
    pasta = entrada / 'cnpj'
    arqvs = os.listdir(pasta)

    #obter arquivos de interesse
    empresas = os.path.join(pasta, arqvs[-1])
    socios = os.path.join(pasta, arqvs[1])

    #carregar base de candidatos
    p = 'politicos_2016_2018_100maiorescidades_para_producao'
    candidatos = pd.read_excel(entrada / p)
    candidatos['NM_CANDIDATO'] = candidatos['NM_CANDIDATO'].apply(utf8_ascii)
    cpfs = set(candidatos['CPF_mascara'].astype(str).to_list())
    nomes = set(candidatos['NM_CANDIDATO'])

    #criar bancos de dados vazio
    socios_final, empresas_final = pd.DataFrame(), pd.DataFrame()

    #criar argumentos para processar csvs
    kwargs = {'chunksize': chunksize, 'sep': '#', 'dtype': str}
    kwargs_= {'index': False, 'quoting': 2}

    #carregar base de socios da receita federal
    reader = pd.read_csv(socios, **kwargs)

    #iterar o arquivo csv em chunks de 1.000.000 linhas
    for chunk in tqdm(reader):

        #filtrar por cpfs
        chunk = chunk[(
            chunk['cnpj_cpf_socio'].isin(cpfs) &
            chunk['nome_socio'].isin(nomes)
        )]

        #pular etapas se não achar ninguém
        if not chunk.empty:

            #encontrar donos de empresas na base de políticos
            socios_final = pd.concat(
                [socios_final, chunk], ignore_index=True
            )

    if not socios_final.empty:

        colunas = list(socios_final.columns) + ['NR_CPF_CANDIDATO']
        socios_final = socios_final.merge(
            candidatos,
            how='inner',
            left_on=['cnpj_cpf_socio', 'nome_socio'],
            right_on=['CPF_mascara', 'NM_CANDIDATO']
        )
        socios_final = socios_final[colunas]
        socios_final\
            .reset_index(drop=True)\
            .to_csv(saida / 'rf_socios.csv', **kwargs_)

    #extrair os cnpjs dos socios
    cnpjs = set(socios_final['cnpj'].astype(str))

    #carregar base de empresas
    reader = pd.read_csv(empresas, **kwargs)

    #iterar o arquivo csv em chunks de 1.000.000 linhas
    for chunk in tqdm(reader):

        #filtrar por cpfs
        chunk = chunk[chunk['cnpj'].astype(str).isin(cnpjs)]

        #pular etapas se não achar ninguém
        if not chunk.empty:

            #encontrar donos de empresas na base de políticos
            empresas_final = pd.concat(
                [empresas_final, chunk], ignore_index=True
            )

    if not empresas_final.empty:
        empresas_final\
            .reset_index(drop=True)\
            .to_csv(saida / 'rf_empresas.csv', **kwargs_)

    #organizar base 5
    organizar_base05()

## script para organizar os dados do tse
#reinaldo chaves: reichaves@gmail.com
#andre assumpcao: andre.assumpcao@gmail.com
#descrição
# programa para organizar informações dos processos dos políticos.
# dados vêm do digesto.

#importar bibliotecas principais
from pathlib import Path
from csv import QUOTE_ALL, QUOTE_NONNUMERIC, reader
from datetime import datetime
from tqdm import tqdm
from unicodedata import normalize
import ast
import pandas as pd, numpy as np
import re, os, shutil
import zipfile

#criar variáveis compartilhadas por todos os métodos
entrada = Path('dados/entrada/')
saida = Path('dados/saida/')

#criar função para carregar todos os bancos na memória
def carregar_bancos():
    bancos = [
        f'{os.path.abspath(".")}/{str(saida)}/politicos_{banco}.csv'
        for banco in ['detalhes', 'movs', 'partes']
    ]
    return [pd.read_csv(banco, **kwargs) for banco in bancos]

#corrige colunas de cnj
def corrige_cnj(coluna):
    return coluna.str.replace('[_\\.\\-/]', '')

#conserta as datas das movimentações para o paulo
def conserta_data(data):
    return f'{data[-2:]}/{data[5:7]}/{data[:4]}'

#converte coluna para datetime format
def converte_coluna(coluna):
    return coluna.astype('datetime64[ms]')

#converter true e false para rótulos do site
def converter_boolean(x):
    x = 'SIM' if x in ['True', '1.0'] else 'NÃO'
    return x

# definir função para apagar acentos de nomes de políticos
def utf8_ascii(var):
    return normalize('NFKD', var).encode('ascii', 'ignore').decode('utf-8')

#criar função para organizar base01_processos
def organizar_base01(detalhes, candidatos, partes, assuntos, retorno=False):

    #filtrar apenas os casos de interesse
    detalhes = detalhes[detalhes['status_publiquese'] != '2']
    detalhes.drop('status_publiquese', axis=1, inplace=True)

    #selecionar colunas para manter
    detalhes = detalhes[[
        'comarca', 'foro', 'tribunal', 'uf', 'acessos', 'vara', 'cpf',
        'numero_cnj', 'area', 'arquivado', 'distribuicaoData',
        'extinto', 'juiz', 'numeroAlternativo', 'processoID',
        'segredo_justica', 'sentencaData', 'situacao', 'valor'
    ]]

    #renomear variáveis para bater banco do digesto
    detalhes.rename(
        columns={
            'distribuicaoData' : 'data_distribuicao',
            'juiz'             : 'magistrado',
            'numeroAlternativo': 'numero_alternativo',
            'sentencaData'     : 'data_sentenca',
            'acessos'          : 'data_atualizacao'
        },
        inplace=True
    )

    #criar variáveis
    detalhes['numero_unico_trib'] = \
        detalhes['numero_cnj'].replace('[_\\.\\-/]', '', regex=True)
    detalhes['processo_principal'] = '-'
    detalhes['relacao_parte_monitorada'] = '-'
    detalhes['polo_parte_monitorada'] = '-'

    #carregar banco com links dos processos
    pje = pd.read_csv(entrada / 'numero_unico.csv', dtype=str)
    pje['id_tribunal'] = pje['identificador_tribunal'].apply(lambda x: f'.{x}.')

    #adicionar links
    links = detalhes['numero_cnj'].copy().to_frame()
    links['id_tribunal'] = detalhes['numero_cnj'].str[15:21]
    links = links.merge(pje, on='id_tribunal', how='left')

    #formatar links e corrigir erros
    links['link'] = links['link'].fillna('Indisponível')
    detalhes['link_processo'] = links['link'].to_list()

    #salvar colunas que devem ser mantidas
    extras = ['nome_parte', 'nome_parte_oficial_tse']
    colunas = list(detalhes.columns) + extras

    #juntar com partes para puxar as informações das partes
    processos = detalhes.merge(partes, how='inner', on='cpf')

    #juntar com candidatos para puxar nomes de urna
    processos = processos.merge(
        candidatos[['NM_URNA_CANDIDATO', 'NR_CPF_CANDIDATO']],
        how='inner', left_on='cpf', right_on='NR_CPF_CANDIDATO'
    )

    #renomear e eliminar colunas
    processos['nome_parte'] = processos.nome_da_parte
    processos['nome_parte_oficial_tse'] = processos.NM_URNA_CANDIDATO
    processos.drop(columns=['numero_unico_trib_y', 'numero_cnj_y'])
    processos.rename(
        columns={
            'numero_unico_trib_x': 'numero_unico_trib',
            'numero_cnj_x': 'numero_cnj'
        },
        inplace=True
    )

    #puxar os assunos
    assuntos = assuntos[['numero_unico_trib', 'natureza', 'assunto_extra']]
    processos = processos.merge(assuntos, how='inner', on='numero_unico_trib')
    colunas += ['natureza', 'assunto_extra']

    # selecionar as colunas corretas, mudar o nome e finalizar
    processos = processos[colunas]
    processos[['arquivado', 'segredo_justica', 'extinto']] = \
        processos[['arquivado', 'segredo_justica', 'extinto']].applymap(
            lambda x: 'NÃO' if x in ['False', '0'] else 'SIM'
        )

    #resolver a data de atualização
    def _consertar_data(linha):
        linha = sorted(linha, reverse=True)[0]
        linha = f'{linha[8:10]}/{linha[5:7]}/{linha[:4]} {linha[11:]}'
        return linha

    #consertar linha
    processos['data_atualizacao'] = processos['data_atualizacao']\
        .apply(ast.literal_eval)\
        .apply(_consertar_data)

    #criar path de destino
    fname = saida / 'base01_processos.csv'

    #criar coluna no banco de dados, resetar o índice e salvar em csv
    processos.drop_duplicates(['numero_cnj'], inplace=True, ignore_index=True)
    processos.to_csv(fname, quoting=QUOTE_ALL, index=False)

    #retornar base se necessário
    if retorno: return processos

#criar função para organizar base02
def organizar_base02(movimentacoes):

    #apagar colunas desnecessárias
    movimentacoes.drop(columns=['assuntos', 'id'], inplace=True)

    #criar objeto com as colunas
    colunas = movimentacoes.columns.tolist()
    dupli = [colunas[0]] + [colunas[2]] + [colunas[4]]

    #ordenar movimentações
    movimentacoes = movimentacoes.sort_values(
        ['instancia', 'data_da_movimentacao'], ascending=False
    )

    #corrigir a data para DD-MM-YYYY
    movimentacoes['data_da_movimentacao'] = \
        movimentacoes['data_da_movimentacao'].apply(conserta_data)

    #corrigir cnjs
    numeros_unicos = corrige_cnj(movimentacoes['numero_cnj']).to_list()

    #criar path de destino
    fname = saida / 'base02_movimentacoes.csv'

    #criar coluna no banco de dados, resetar o índice e salvar em csv
    movimentacoes['numero_unico_trib'] = numeros_unicos
    movimentacoes = movimentacoes.drop_duplicates(dupli, ignore_index=True)
    movimentacoes.to_csv(fname, quoting=QUOTE_ALL, index=False)

#criar função para organizar base03
def organizar_base03(partes, retorno=False):

    #apagar variáveis desnecessárias
    apagar = [
        'advogados', 'cep', 'id1', 'nome_texto', 'relacaoID', 'pessoa_fisica',
        'processoID', 'status_politico'
    ]
    partes.drop(columns=apagar, inplace=True)

    #carregar os tipos de parte reportados pelo digesto
    partes_tipo = pd.read_csv(entrada / 'partes_tipo.csv')
    partes = partes.merge(
        partes_tipo, left_on='relacao_normalizado', right_on='descricao',
        how='left'
    )

    #substituir o tipo da parte e eliminar key
    partes.drop(columns=['descricao', 'relacao_normalizado'], inplace=True)

    #renomear variáveis restantes
    partes.rename(
        columns={
            'nome_normalizado': 'nome_da_parte', 'doc': 'doc_completo_parte',
            'tipo': 'tipo_da_parte'
        },
        inplace=True
    )

    #corrigir cnjs
    numeros_unicos = corrige_cnj(partes['numero_cnj']).to_list()

    #converter documento da parte
    partes['doc_completo_parte_limpo'] = corrige_cnj(
        partes['doc_completo_parte']
    )
    col = ['coautor', 'reu', 'neutro']
    partes[col] = partes[col].applymap(converter_boolean)

    #criar path de destino
    fname = saida / 'base03_partes.csv'

    #criar coluna no banco de dados, resetar o índice e salvar em csv
    partes['numero_unico_trib'] = numeros_unicos
    partes = partes.drop_duplicates(['numero_cnj', 'nome_da_parte'])
    partes.to_csv(fname, quoting=QUOTE_ALL, index=False)

    #retornar se necessário
    if retorno: return partes

#criar função para organizar base04
def organizar_base04(refazer=False, retorno=False):

    #eliminar estes passos se o banco já tiver sido descomprimido
    if refazer:

        #criar função para descomprimir os arquivos do site do tse.
        def _descomprimir_dados(path):

            #buscar arquivos e organizá-los
            anos = [2016, 2018]
            arqvs = os.listdir(path)
            regex = re.compile(r'\.zip$')
            arqvs = sorted(list(filter(regex.search, arqvs)))
            arqvs = [f'{path}{arqv}' for arqv in arqvs]

            #executar descompressão
            for ano, arqv in zip(anos, arqvs):
                os.mkdir(f'{path}eleicao{ano}')
                with zipfile.ZipFile(arqv, 'r') as z:
                    z.extractall(f'{path}eleicao{ano}')

        #criar função para juntar os dados por eleição
        def _juntar_dados(path):

            #definir parâmetros de limpeza de dados
            anos = [2016, 2018]
            kwargs = {
                'index_col': False, 'encoding': 'latin1', 'engine': 'python',
                'quotechar': '"', 'quoting': QUOTE_ALL, 'doublequote': False,
                'dtype': str
            }
            regex = re.compile(r'(BR|BRASIL)\.csv$')
            eleições = [f'{path}eleicao{str(ano)}' for ano in anos]
            for eleição, ano in zip(eleições, anos):
                arqvs = [f'{eleição}/{f}' for f in os.listdir(eleição)]
                arqvs = [arqv for arqv in arqvs if not re.search(r'pdf$',arqv)]
                arqvs = pd.concat(
                    [pd.read_csv(arqv, ';', **kwargs) for arqv in arqvs]
                )
                arqvs.to_csv(f'{path}candidatos{str(ano)}.csv', index=False)

        #criar função para apagar arquivos inúteis
        def _apagar_arquivos(path):
            anos = [2016, 2018]
            pastas = [f'{path}eleicao{str(ano)}' for ano in anos]
            for pasta in pastas:
                shutil.rmtree(pasta)

        #executar funções externas
        _descomprimir_dados(entrada)
        _juntar_dados(entrada)
        _apagar_arquivos(entrada)

    #carregar base de políticos
    politicos = pd.read_excel(
        entrada / 'politicos_2016_2018_100maiorescidades_para_producao.xlsx',
        converters={'NR_CPF_CANDIDATO': lambda x: str(x).rjust(11)}
    )

    #construir lista de cpfs
    politicos['NR_CPF_CANDIDATO'] = (
        politicos['NR_CPF_CANDIDATO']
            .apply(lambda x: str(x) if not pd.isnull(x) else x)
            .replace(r' *', '', regex=True)
            .str.pad(11, fillchar='0')
    )

    politicos = politicos.NR_CPF_CANDIDATO.to_list()

    #carregar dados e filtrar de acordo com os CPFs
    cand2014 = pd.read_csv(
        entrada / 'candidatos2014.csv', dtype=str, sep=';', encoding='latin1'
    )
    cand2016 = pd.read_csv(entrada / 'candidatos2016.csv', dtype=str)
    cand2018 = pd.read_csv(entrada / 'candidatos2018.csv', dtype=str)

    #corrigir nome de coluna errado
    cand2018['NR_PROCESSO'] = cand2018['NR_PROCESSO']\
        .fillna(cand2018['NR_PROCESSO '])
    cand2018 = cand2018[cand2018.columns[:-1]]

    #salvar base sem alterações
    organizar_base04.candidatos = pd.concat(
        [cand2014, cand2016, cand2018], ignore_index=True
    )

    #salvar base com alterações
    candidatos = organizar_base04.candidatos[
        organizar_base04.candidatos['NR_CPF_CANDIDATO'].isin(politicos)
    ]

    #filtrar colunas
    colunas = [
        'SG_UF', 'DS_CARGO', 'SQ_CANDIDATO', 'ANO_ELEICAO', 'NM_CANDIDATO',
        'NM_URNA_CANDIDATO', 'NR_CPF_CANDIDATO', 'SG_PARTIDO',
        'SG_UF_NASCIMENTO', 'NM_MUNICIPIO_NASCIMENTO', 'DT_NASCIMENTO',
        'DS_GENERO', 'DS_SIT_TOT_TURNO'
    ]
    candidatos = candidatos[colunas]
    candidatos['CPF_mascara'] = [
        f'***{cpf}**' for cpf in candidatos.NR_CPF_CANDIDATO.str[3:9].to_list()
    ]

    candidatos = candidatos\
        .sort_values(['ANO_ELEICAO', 'DS_SIT_TOT_TURNO'])\
        .drop_duplicates('NR_CPF_CANDIDATO', keep='last')

    #criar variável slug para paulo e mudar nome dos candidatos
    candidatos['NM_CANDIDATO'] = candidatos['NM_CANDIDATO'].apply(utf8_ascii)
    slug = candidatos['NM_CANDIDATO'].to_list()
    slug = list(map(utf8_ascii, slug))
    slug = [re.sub(r' ', '-', candidato.lower()) for candidato in slug]
    candidatos['slug'] = slug

    #criar path de destino
    fname = saida / 'base04_politicos.csv'

    #criar coluna no banco de dados, resetar o índice e salvar em csv
    candidatos.to_csv(fname, quoting=QUOTE_ALL, index=False)

    #retornar se necessário
    if retorno: return candidatos

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
    rf_empresas['data_atualizacao'] = '28/04/2020'
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

# criar função para organizar base06
def organizar_base06(partes, retorno=False):

    # subir base de políticos
    politicos = pd.read_csv(saida / 'base04_politicos.csv', dtype=str)

    #substituir o tipo da parte e eliminar key
    partes.rename(columns={'tipo_da_parte': 'parte_tipo'}, inplace=True)
    partes['parte_tipo'] = partes['parte_tipo'].fillna('Outro(a)')

    #juntar políticos com partes com base nos cpfs
    politicos = politicos[['NR_CPF_CANDIDATO', 'DS_CARGO']]
    partes = partes.reset_index()
    idx = partes.merge(politicos, left_on='cpf', right_on='NR_CPF_CANDIDATO')
    idx = idx.set_index('index', drop=True)
    del idx['cpf']
    idx.rename(columns={'NR_CPF_CANDIDATO': 'cpf'}, inplace=True)

    #criar variáveis extras necessárias para paulo
    idx['polo_politico'] = 'SIM'
    partes['polo_politico'], partes['DS_CARGO'] = 'NÃO', np.NaN
    partes = partes[~partes['index'].isin(idx.index)]
    del partes['index']

    #arrumar últimos problemas com índice dos bancos de dados
    idx = idx.reset_index(drop=True)
    idx = idx[partes.columns]
    partes = pd.concat([partes, idx], ignore_index=True)
    partes.rename(columns={'DS_CARGO': 'cargo_politico'}, inplace=True)

    #manter apenas algumas variáveis
    manter = [
        'numero_unico_trib', 'nome_da_parte', 'cpf', 'parte_tipo',
        'polo_politico'
    ]
    partes = partes[manter]

    # criar path de destino
    fname = saida / 'base06_partes_todas.csv'

    # criar coluna no banco de dados, resetar o índice e salvar em csv
    partes = partes.drop_duplicates(['numero_unico_trib', 'nome_da_parte'])
    partes.to_csv(fname, quoting=QUOTE_ALL, index=False)

    # retornar base se necessário
    if retorno: return partes

#definir função para limpar os assuntos extras
def organizar_base07(detalhes, retorno=False):

    #copiar o banco de detalhes
    df = detalhes.copy()

    #filtrar apenas os casos de interesse
    df = df[df['status_publiquese'] != '2']
    df = df.drop('status_publiquese', axis=1)

    #adicionar ao banco
    df['natureza'] = df['classeNatureza'].to_list()
    df['assunto_extra'] = df['assuntoExtra'].to_list()

    #criar novo banco de assuntos
    assuntos = df[['natureza', 'assunto_extra', 'numero_cnj']]
    assuntos.columns = ['natureza', 'assunto_extra', 'numero_unico_trib']
    assuntos['numero_unico_trib'] = \
        assuntos['numero_unico_trib'].str.replace(r'\W', '', regex=True)
    assuntos.drop_duplicates(
        'numero_unico_trib', ignore_index=True, inplace=True
    )

    #criar path de destino
    fname = saida / 'base07_assuntos.csv'

    #criar coluna no banco de dados, resetar o índice e salvar em csv
    assuntos.to_csv(fname, quoting=QUOTE_ALL, index=False)

    # retornar base se necessário
    if retorno: return assuntos

# criar função para organizar base08
def organizar_base08():
    fname = saida / 'base08_qtdeprocessos.csv'
    df = pd.read_csv(saida / 'politicos_partes.csv', dtype=str)
    df = df[['numero_cnj', 'cpf', 'status_politico']]
    df = df[df['cpf'].notnull()]
    df = df[df['status_politico'] != '2']
    df['numero_cnj'] = df['numero_cnj'].replace(r'\W', '', regex=True)
    df\
        .drop_duplicates(['numero_cnj', 'cpf'])\
        .drop(columns=['status_politico'])\
        .groupby('cpf')\
        .count()\
        .reset_index()\
        .rename(columns={'numero_cnj': 'qt_processos'})\
        .to_csv(fname, quoting=QUOTE_ALL, index=False)
    return df

# criar função para organizar base09
def organizar_base09(df):
    fname = saida / 'base09_listaprocessos.csv'
    df\
        .drop_duplicates(['numero_cnj', 'cpf'])\
        .rename(columns={'numero_cnj': 'numero_unico_trib'})\
        [['cpf', 'numero_unico_trib']]\
        .to_csv(fname, quoting=QUOTE_ALL, index=False)

# criar base de dados para download
def organizar_base10(
    assuntos=['Corrupção', 'Improbidade Administrativa'],
    processos=pd.read_csv(saida / 'base01_processos.csv', dtype=str),
    movs=pd.read_csv(saida / 'base02_movimentacoes.csv', dtype=str),
    partes=pd.read_csv(saida / 'base03_partes.csv', dtype=str)
):

    #verificar quais assuntos foram passados e criar proxy se nenhum
    #assunto tiver sido passado
    if not assuntos:
        assuntos = 'NENHUM_ASSUNTO'
    elif isinstance(assuntos, list):
        assuntos = f'({"|".join(assuntos)})'
    else:
        pass

    #procurar assuntos de interesse e selecionar processos de acordo com
    #os resultados
    processos = processos.dropna(subset=['assunto_extra'])
    processos = processos[
        processos['assunto_extra'].str.contains(assuntos, regex=True)
    ]

    #eliminar quebras de linha
    processos['assunto_extra'] = processos['assunto_extra']\
        .str.replace('<br/>', '; ')

    #usar cnj para filtrar as partes e as movimentações
    cnjs = set(processos['numero_cnj'].to_list())
    partes = partes[partes['numero_cnj'].isin(cnjs)]
    movs = movs[movs['numero_cnj'].isin(cnjs)]

    #juntar processos e partes no mesmo download
    download = processos.merge(partes, on='numero_cnj', how='left')
    download.rename(
        columns={'cpf_x': 'cpf', 'numero_unico_trib_x': 'numero_unico_trib'},
        inplace=True
    )
    del download['cpf_y']
    del download['numero_unico_trib_y']

    #preparar os downloads
    fname01 = saida / 'download_processos.csv'
    fname02 = saida / 'download_movimentacoes.csv'

    #salvar em disco
    download.to_csv(fname01, quoting=QUOTE_ALL, index=False)
    movs.to_csv(fname02, quoting=QUOTE_ALL, index=False)

# bloco de execução principal
if __name__ == '__main__':

    #ignorar warning do pandas
    pd.options.mode.chained_assignment = None

    #definir objetos comuns à análise
    kwargs = {
        'index_col': False, 'quoting': 1, 'low_memory': False, 'dtype': str
    }

    #carregar todos os bancos
    detalhes, movs, partes = carregar_bancos()

    #criar base02_candidatos
    organizar_base02(movs)
    print('base02 pronta.')

    #criar base03_partes
    partes = organizar_base03(partes, True)
    print('base03 pronta.')

    #criar base04_candidatos
    organizar_base04()
    print('base04 pronta.')

    #criar base05_politicos_empresas
    organizar_base05()
    print('base05 pronta.')

    #criar base06_partes
    organizar_base06(partes)
    print('base06 pronta.')

    #criar base07_partes
    assuntos = organizar_base07(detalhes, True)
    print('base07 pronta.')

    #criar base01_processos
    organizar_base01(detalhes, organizar_base04.candidatos, partes, assuntos)
    print('base01 pronta.')

    #criar base08_qtprocessos
    processos = organizar_base08()
    print('base08 pronta.')

    #criar base09_listaprocessos
    organizar_base09(processos)
    print('base09 pronta.')

    #criar base10_downloads
    # assuntos = ['Calúnia', 'Difamação', 'Injúria', '']
    organizar_base10()
    print('base10 pronta.')

    # print número de processos e políticos
    politicos = pd.read_csv('dados/saida/base08_qtdeprocessos.csv')
    print(
        f'Número único de políticos: \
        {len(politicos.drop_duplicates("cpf"))}.'
    )
    processos = pd.read_csv('dados/saida/base09_listaprocessos.csv')
    print(
        f'Número único de processos: \
        {len(processos.drop_duplicates("numero_unico_trib"))}.'
    )


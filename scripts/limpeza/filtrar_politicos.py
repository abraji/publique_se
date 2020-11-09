# filtrar os dados mais importantes do digesto
# autor: andre assumpcao
# andre.assumpcao@gmail.com

#importar pacotes
from pathlib import Path
import ast, multiprocessing as mp, os, re, pickle

#importar pacotes de terceiros
import Levenshtein
import pandas as pd, numpy as np
import tqdm

#desligar warning do pandas
pd.set_option('mode.chained_assignment', None)

#definir método para separar assuntos jurídicos
def _split_assunto(textos, ramo_sim):
    texto = re.split(r'\||,(?! )', textos)
    texto = [
        item.upper() if item.upper() in ramo_sim else item for item in texto
    ]
    texto = [item.strip() for item in texto]
    texto = list(filter(None, texto))
    return texto

#definir método para confirmar se assunto é de interesse do publique-se
def confirmar_interesse(textos, assuntos, ramo_sim,  ramo_nao):
    sim = len(set(_split_assunto(textos, ramo_sim)) & assuntos) > 0
    nao = len(set(_split_assunto(textos, ramo_sim)) & ramo_nao) == 0
    return sim and nao

#achar políticos por nome completo
def encontrar_nomes(nomes, cands, partes, frequência=1):

    #separar os nomes por frequência
    nomes_unicos = nomes[nomes['n'] == frequência]
    cands_unicos = cands[cands['nome'].isin(nomes_unicos['nome'])]
    # cands = candidatos.copy()
    # frequência=1
    #corrigir o domicílio eleitoral dos candidatos a presidente e vice
    # if candidatos_info:
    # presidenciaveis = cands_unicos[cands_unicos['SG_UF'] == 'BR'].index
    # cands_unicos.loc[presidenciaveis, 'SG_UF'] = [
    #     'PR', 'SP', 'RS', 'SP', 'SP', 'RJ', 'SP', 'AC', 'CE', 'RJ', 'RJ', 'ES',
    #     'RS', 'SP', 'DF', 'RS', 'SP', 'PE', 'SP', 'SP', 'RJ', 'SP', 'GO', 'SP',
    #     'SP', 'MA', 'RS'
    # ]
    presidenciaveis = cands_unicos[cands_unicos['SG_UF'] == 'BR']
    domicilio_eleitoral = [
        ('JOSE MARIA EYMAEL', 'SP'),
        ('CHRISTIAN LOHBAUER', 'SP'),
        ('ANA AMELIA DE LEMOS', 'RS'),
        ('MARIA OSMARINA MARINA DA SILVA VAZ DE LIMA', 'AC'),
        ('CIRO FERREIRA GOMES', 'CE'),
        ('BENEVENUTO DACIOLO FONSECA DOS SANTOS', 'RJ'),
        ('PAULO RABELLO DE CASTRO', 'RJ'),
        ('LEO DA SILVA ALVES', 'ES'),
        ('GERMANO ANTONIO RIGOTTO', 'RS'),
        ('ANTONIO HAMILTON MARTINS MOURAO', 'DF'),
        ('JOAO VICENTE FONTELLA GOULART', 'RS'),
        ('HENRIQUE DE CAMPOS MEIRELLES', 'SP'),
        ('SUELENE BALDUINO NASCIMENTO', 'PE'),
        ('HERTZ DA CONCEICAO DIAS', 'SP'),
        ('JAIR MESSIAS BOLSONARO', 'RJ'),
        ('FERNANDO HADDAD', 'SP'),
        ('GERALDO JOSE RODRIGUES ALCKMIN FILHO', 'SP'),
        ('KATIA REGINA DE ABREU', 'GO'),
        ('EDUARDO JORGE MARTINS ALVES SOBRINHO', 'SP'),
        ('JOAO DIONISIO FILGUEIRA BARRETO AMOEDO', 'SP'),
        ('LUIZ INACIO LULA DA SILVA', 'SP'),
        ('HELVIO COSTA DE OLIVEIRA TELLES', 'MA'),
        ('SONIA BONE DE SOUSA SILVA SANTOS', 'RS')
    ]

    # mudar o domicílio eleitoral
    for pessoa, uf in domicilio_eleitoral:
        presidenciaveis.loc[presidenciaveis['nome1']==pessoa, 'SG_UF'] = uf

    # substituir os estados
    cands_unicos.loc[presidenciaveis.index, 'SG_UF'] = presidenciaveis.SG_UF

    #criar lista de prováveis políticos
    provaveis = partes[partes['cpf'].isnull()]
    provaveis = provaveis[
        provaveis['nome_normalizado'].isin(cands_unicos['nome'])
    ]
    cands_unicos = cands_unicos.merge(tribunais_uf, on='SG_UF')

    #selecionar lista para match
    cands_unicos = cands_unicos[['NR_CPF_CANDIDATO', 'nome', 'tribunais']]
    provaveis = provaveis[['index', 'nome_normalizado', 'numero_cnj']]

    #criar lista de correspondência
    provaveis = provaveis.merge(
        cands_unicos, left_on='nome_normalizado', right_on='nome'
    )
    provaveis = provaveis[[
        'index', 'nome_normalizado', 'NR_CPF_CANDIDATO', 'numero_cnj',
        'tribunais'
    ]]

    #transformar listas de correspondência
    cnj = provaveis['numero_cnj'].str[15:21].to_list()
    trib = provaveis['tribunais'].to_list()

    #salvar linhas em que a correspondência é exata
    linhas = [True if proc in trb else False for proc, trb in zip(cnj, trib)]

    #criar banco para politicos com nomes completos e cujo processo
    #corre no próprio município.
    resultado = provaveis.loc[linhas, ['index', 'NR_CPF_CANDIDATO']]

    #retornar resultado
    return resultado

#definir função para formatar assuntos
def formatar_assuntos(texto):
    assuntos = organizar_assuntos(texto)
    assuntos = sorted(list(set(assuntos)))
    assuntos = [re.split(r'\|', assunto, maxsplit=1) for assunto in assuntos]
    ramos, assunto_detalhados = [], []
    for assunto in assuntos:
        if len(assunto) == 2:
            ramos += [assunto[0]]
            assunto_detalhados += [assunto[1]]
        elif assunto[0].isupper():
            ramos += [assunto[0]]
        else:
            assunto_detalhados += [assunto[0]]
    ramos = sorted(list(set(ramos)))
    assunto_detalhados = sorted(list(set(assunto_detalhados)))
    ramos = [ramo.strip() for ramo in ramos]
    assunto_detalhados = [assunto.strip() for assunto in assunto_detalhados]
    ramos = '<br/>'.join(ramos)
    assunto_detalhados = '<br/>'.join(assunto_detalhados)
    return (ramos, assunto_detalhados)

#definir método para filtrar os assuntos que não sejam nulos
def filtrar_assuntos(df, descarte):
    return df[~df['numero_cnj'].isin(descarte['numero_cnj'])]

#definir função para organizar assuntos que são extraídos do digesto
def organizar_assuntos(texto):
    texto = _split_assunto(texto, ramo_sim)
    lista = []
    for i, assunto in enumerate(texto):
        if assunto.isupper() or i == 0:
            lista += [assunto]
        else:
            lista[len(lista)-1] = lista[len(lista)-1] + f' | {assunto}'
    return lista

#definir método principal
if __name__ == '__main__':

    #definir objetos comuns à análise
    kwargs = {
        'index_col': False, 'quoting': 1, 'low_memory': False, 'dtype': str
    }
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')

    #carregar bancos de processos
    detalhes = pd.read_csv(entrada / 'politicos_detalhes.csv', **kwargs)
    partes = pd.read_csv(entrada / 'politicos_partes.csv', **kwargs)

    #confirmar processos nas partes
    #puxar os cnpjs da base de empresas
    rf_empresas = pd.read_csv(saida/'base05_politicos_empresas.csv', **kwargs)

    #carregar os bancos dos candidatos
    candidatos = pd.read_csv(saida / 'candidatos_info.csv', dtype=str)
    nomes = pd.read_csv(saida / 'candidatos_nomes.csv')

    #acertar cpf dos candidatos
    candidatos['NR_CPF_CANDIDATO'] = (candidatos['NR_CPF_CANDIDATO']
        .apply(lambda x: str(x) if not pd.isnull(x) else x)
        .replace(r'\.\d', '', regex=True)
        .str.pad(11, fillchar='0')
    )

    #carregar o indicator de tribunais
    tribunais_uf = pd.read_csv(entrada / 'tribunais_uf.csv')
    tribunais_uf['tribunais'] = tribunais_uf['tribunais'].apply(
        ast.literal_eval
    )

    #atualizar o banco de partes com os cpfs já existentes
    partes['status_politico'] = 2
    cpfs = candidatos.NR_CPF_CANDIDATO.to_list()
    cpfs = [cpf.rjust(11, '0') for cpf in cpfs]
    partes.loc[partes['cpf'].isin(cpfs), 'status_politico'] = 3

    #filtrar os cnpjs e selecionar os cpfs associados a eles
    partes_cnpj = partes.merge(rf_empresas, on='cnpj')
    partes_cnpj = partes_cnpj[['numero_cnj', 'cnpj', 'NR_CPF_CANDIDATO']]
    partes_cnpj = partes_cnpj.sort_values('cnpj')

    #puxar os índices dos cnpjs e substituir os cpfs onde houver match de
    # cnpj
    idx00 = partes[partes['cnpj'].isin(partes_cnpj['cnpj'])]
    idx00 = idx00.sort_values('cnpj').index
    partes.loc[idx00, 'cpf'] = partes_cnpj.NR_CPF_CANDIDATO.to_list()
    partes.loc[idx00, 'status_politico'] = 3

    #encontrar as pares por nome completo
    partes = partes.reset_index()
    idx01 = encontrar_nomes(nomes, candidatos, partes, 1)
    idx02 = encontrar_nomes(nomes, candidatos, partes, 2)
    idx03 = encontrar_nomes(nomes, candidatos, partes, 3)
    idx04 = encontrar_nomes(nomes, candidatos, partes, 4)
    idx05 = encontrar_nomes(nomes, candidatos, partes, 5)

    #substituir os cpfs dos nomes encontrados em frequência decrescente
    # partes.loc[idx05['index'], 'cpf'] = idx05['NR_CPF_CANDIDATO'].to_list()
    # partes.loc[idx04['index'], 'cpf'] = idx04['NR_CPF_CANDIDATO'].to_list()
    # partes.loc[idx03['index'], 'cpf'] = idx03['NR_CPF_CANDIDATO'].to_list()
    # partes.loc[idx02['index'], 'cpf'] = idx02['NR_CPF_CANDIDATO'].to_list()
    partes.loc[idx01['index'], 'cpf'] = idx01['NR_CPF_CANDIDATO'].to_list()

    #substituir método de identificação
    partes.loc[idx05['index'], 'status_politico'] = 4
    partes.loc[idx04['index'], 'status_politico'] = 4
    partes.loc[idx03['index'], 'status_politico'] = 4
    partes.loc[idx02['index'], 'status_politico'] = 4
    partes.loc[idx01['index'], 'status_politico'] = 4

    # carregar o banco de nomes incomuns
    incomuns_nomes = pd.read_csv(saida / 'incomuns_nomes.csv', dtype=str)
    incomuns_nomes['NR_CPF_CANDIDATO'] = (
        incomuns_nomes['NR_CPF_CANDIDATO']
            .apply(lambda x: str(x) if not pd.isnull(x) else x)
            .replace(r'\.\d', '', regex=True)
            .str.pad(11, fillchar='0')
    )

    # concatenar os sobrenomes de cada político
    incomuns_nomes['sobrenomes_parciais'] = (
        incomuns_nomes['sobrenomes_parciais'].apply(ast.literal_eval)
    )

    # criar regex para cada político a partir de seus sobrenomes
    incomuns_nomes['regex'] = incomuns_nomes['sobrenomes_parciais'].apply(
        lambda x: (f'.*\\b({"|".join(x)})\\b' if len(x) > 0 else None)
    )
    incomuns_nomes = incomuns_nomes[incomuns_nomes['regex'].notnull()]

    # compor o nome completo para busca, incluindo os sobrenomes
    incomuns_nomes['regex'] = (
        '^'+incomuns_nomes['nome'].str.cat(incomuns_nomes['regex'])
    )

    # filtrar o banco de partes apenas para os casos que ainda não foram
    #  identificados
    incomuns_partes = partes[partes['status_politico']==2]
    incomuns_partes = (
        incomuns_partes[incomuns_partes['nome_normalizado'].notnull()]
    )
    incomuns_tuple = incomuns_partes[['index', 'nome_normalizado']]

    # separar nomes e sobrenomes
    lista01 = list(incomuns_nomes['regex'].to_frame().itertuples(name=None))
    lista02 = list(incomuns_tuple.itertuples(name=None, index=False))

    # adicionar checagem alfabética para agilizar correspondência
    lista01 = sorted(
        [(*element, element[1][1]) for element in lista01], key=lambda x: x[1]
    )
    lista02 = sorted(
        [(*element, element[1][0]) for element in lista02], key=lambda x: x[1]
    )

    # # achar correspondência
    # correspondências = [] #, reindex = ['a', 'A', 'A'], 0
    # for item01 in tqdm.tqdm(lista01):
    #     for item02 in lista02:
    #         if item01[2] == item02[2]:
    #             if re.search(re.compile(item01[1]), item02[1]):
    #                 correspondências.append((item01[0], item02[0]))

    # # salvar em disco
    # with open(saida / f'correspondência_nomesparcias.pickle', 'wb') as fp:
    #     pickle.dump(correspondências, fp)

    # puxar o arquivo com as correspondências e uni-los num arquivo só
    with open(saida / f'correspondência_nomesparcias.pickle', 'rb') as fp:
        files = pickle.load(fp)

    # criar o banco de dados de correspondência de nomes parciais
    nomes_possiveis01 = pd.DataFrame(columns=['idx01', 'idx02'], data=files)

    # recriar nomes completos
    incomuns_nomes['nome_completo'] = (
        incomuns_nomes['nome'].astype(str) + ' ' + incomuns_nomes['sobrenomes']
    )

    # puxar os nomes de políticos possíveis
    nomes_possiveis02 = incomuns_nomes.loc[nomes_possiveis01['idx01'].to_list(),:]
    nomes_possiveis02 = nomes_possiveis02[['NR_CPF_CANDIDATO', 'nome_completo']]

    # puxar os nomes de partes possíveis
    nomes_possiveis03 = incomuns_partes.loc[nomes_possiveis01['idx02'].to_list(),:]
    nomes_possiveis03 = nomes_possiveis03[['index', 'nome_normalizado']]

    # eliminar "PRESO" do nome normalizado
    nomes_possiveis03['nome_normalizado'] = (
        nomes_possiveis03['nome_normalizado'].str.replace(
            r' PRESO$', '', regex=True
        )
    )

    # criar o banco de possíveis nomes
    nomes_possiveis = pd.concat([
        nomes_possiveis02.reset_index(drop=True),
        nomes_possiveis03.reset_index(drop=True)
        ], axis=1
    )

    # computar a distância de levenshtein
    match = nomes_possiveis.iloc[:, [1,3]].itertuples(name=None, index=False)
    levenshtein = [Levenshtein.distance(*element) for element in match]
    nomes_possiveis['levenshtein'] = levenshtein

    # preservar apenas nomes com distância de edição menor do que 4
    possiveis = nomes_possiveis[nomes_possiveis['levenshtein'] < 4]
    possiveis = possiveis[['NR_CPF_CANDIDATO', 'index']]
    possiveis = possiveis.reset_index(drop=True)
    possiveis = possiveis.drop_duplicates('index')

    # substituir os potenciais matches de nomes parciais no banco
    # de partes adicionando o CPF
    partes.loc[possiveis['index'], 'cpf'] = possiveis['NR_CPF_CANDIDATO'].to_list()
    partes.loc[possiveis['index'], 'status_politico'] = 5

    # indicar políticos sem adicionar o cpf
    possiveis_semconfirmacao = (
        nomes_possiveis[nomes_possiveis['levenshtein']==4]
    )[['NR_CPF_CANDIDATO', 'index']].drop_duplicates('index')

    # substituir os potenciais matches de nomes parciais no banco
    # de partes sem adicionar o CPF
    partes.loc[possiveis_semconfirmacao['index'], 'status_politico'] = 5

    #jogar fora o índice
    partes = partes.drop('index', axis=1)

    #adicionar os cpfs dos políticos no banco de detalhes
    cnjs = detalhes['numero_cnj'].to_frame()
    todos = cnjs.merge(partes, on='numero_cnj', how='left')

    #pares políticos e processos
    pares = todos.loc[
        todos['status_politico']>2, ['numero_cnj', 'cpf', 'status_politico']
    ]

    #definir amostra final
    detalhes = detalhes.merge(pares, on='numero_cnj', how='left')
    detalhes['status_politico'] = detalhes['status_politico'].fillna(2)
    detalhes = detalhes.drop(columns=['cpf_x', 'status_publiquese'])
    detalhes = detalhes.rename(
        columns={'cpf_y': 'cpf', 'status_politico': 'status_publiquese'}
    )

    #fixas os detalhes dos processos
    detalhes = detalhes.drop_duplicates(['numero_cnj', 'cpf'])

    #imprimir resultado
    resultados = detalhes.groupby('status_publiquese').count()
    resultados = resultados[['numero_cnj', 'cpf']]

    for status in resultados.itertuples(name=None):
        print(
            f'\n Status: {int(status[0])} \n -----------\n',
            f'Nº processos: {status[1]:>5}\n',
            f'Nº políticos: {status[2]:>5}\n\n'
        )

    #importar tabela processual da união
    tpu = pd.read_excel(entrada / 'identificadores_tpu_avaliado.xlsx')

    #definir conjuntos dos ramos do direito e assuntos jurídicos
    ramo_sim = set(tpu.loc[tpu['INTERESSA'] == 'SIM', 'nivel1'])
    ramo_nao = set(tpu.loc[tpu['INTERESSA'] == 'NÃO', 'nivel1']) - ramo_sim
    assuntos = set(tpu.loc[tpu['INTERESSA'] == 'SIM', 'ultimo_assunto'])
    args = (assuntos, ramo_sim, ramo_nao)

    #criar variável com assuntos:
    assuntos_todos = detalhes.assuntoExtra.to_list()
    assuntos_todos = list(map(formatar_assuntos, assuntos_todos))

    #inserir nos dados
    detalhes['classeNatureza'] = [assunto[0] for assunto in assuntos_todos]
    detalhes['assuntoExtra'] =  [assunto[1] for assunto in assuntos_todos]

    #filtrar o banco de detalhes, partes e movs
    # # mask = detalhes[detalhes['status_publiquese'] == 3]
    # partes = partes[partes['numero_cnj'].isin(mask['numero_cnj'])]
    # movs = movs[movs['numero_cnj'].isin(mask['numero_cnj'])]

    #salvar todos os bancos de dados
    kwargs = {'index': False, 'quoting': 1}
    detalhes.to_csv(entrada / 'politicos_detalhesFiltrado.csv', **kwargs)
    partes.to_csv(entrada / 'politicos_partesFiltrado.csv', **kwargs)

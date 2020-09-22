from pathlib import Path
import pandas as pd, numpy as np
import re, os

# paths
entrada = Path('dados/entrada')
saida = Path('dados/saida')

partes = pd.read_csv(saida / 'politicos_partes.csv', dtype=str)
partes[partes['cpf'] == '93565968834']


fname = saida / 'base08_qtdeprocessos.csv'
fname = saida / 'base09_listaprocessos.csv'

qtde = pd.read_csv(fname)
lista = pd.read_csv(fname)
qtde.drop_duplicates('cpf')

lista.drop_duplicates('numero_unico_trib')

df = partes[['numero_cnj', 'cpf', 'status_politico']]
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

processos = pd.read_csv(saida / 'base09_listaprocessos.csv', dtype=str)

processos.drop_duplicates('cpf')

return df

for row in movs[movs['numero_cnj']=='1011567-56.2015.8.26.0011'].iterrows():
    print(row)

movs[movs['numero_cnj']=='1011567-56.2015.8.26.0011'].reset_index().iloc[60:120,:]

# checagem 1 karen
[re.search(r'^base0.*', arqv) for arqv in os.listdir(saida)]
base01 = pd.read_csv(saida / 'base01_processos.csv', dtype=str)
base01[base01['numero_cnj'] == '0016341-85.2018.1.00.0000']\
    .to_dict(orient='records')

base01['cpf'].drop_duplicates()

movs = pd.read_csv(saida / 'base02_movimentacoes.csv', dtype=str)

number = movs['texto_da_movimentacao'].apply(lambda x: len(x) if isinstance(x, str) else 1)
obs = list(number.to_frame().itertuples(name=None))
obs = sorted(obs, key=lambda x: x[1])
obs[-10:]

texto = movs['texto_da_movimentacao'].to_list()
texto = list(map(sys.getsizeof, texto))

texto = list(filter(lambda x: x > 65535, texto))
len(texto)

movs.loc[228667, 'texto_da_movimentacao']

partes = pd.read_csv(saida / 'base03_partes.csv', dtype=str)
sample = substr.dropna(subset=['nome_normalizado'])
sample.loc[sample['nome_normalizado'].str.contains(r'MARCOS.*PASCHOALIN'), ['nome_normalizado', 'cpf', 'status_politico']]

find[find['nome_normalizado'].str.contains(r'MARCOS.*PASCHOALIN')]
substr[substr['nome_normalizado'].str.contains(r'MARCOS.*PASCHOALIN')]
substr_politicos[substr_politicos['nome_normalizado'].str.contains(r'MARCOS.*PASCHOALIN')]
substr_nao_politicos[substr_nao_politicos['nome_normalizado'].str.contains(r'MARCOS.*PASCHOALIN')]

sample = partes.loc[substr.index, ['nome_normalizado', 'cpf', 'status_politico']]
sample.loc[sample['nome_normalizado'].str.contains(r'MARCOS.*PASCHOALIN'), ['nome_normalizado', 'cpf', 'status_politico']]


partes = pd.read_csv(saida / 'base06_partes_todas.csv', dtype=str)
politicos = pd.read_csv(saida / 'base04_politicos.csv', dtype=str)

sample = partes.dropna(subset=['nome_da_parte'])

sample = sample[sample['polo_politico'] == 'SIM'].drop_duplicates('cpf')
sample[sample['nome_da_parte'].str.contains('PASCHOALIN')]
politicos[politicos['NM_CANDIDATO'].str.contains('PASCHOALIN')]

politicos[politicos['NR_CPF_CANDIDATO'] == '24745278691']

base04 = pd.read_csv(saida / 'base04_politicos.csv', dtype=str)
base04[base04['NM_CANDIDATO'].str.contains(r'COLLOR')]

base04 = pd.read_csv(saida / 'base04_politicos.csv', dtype=str)
base04[base04['NM_CANDIDATO'].str.contains(r'ZAMBELLI')]
base04[base04['NM_CANDIDATO'].str.contains(r'ROMERO JUCA')]

set(list(map(len, rf_empresas['NR_CPF_CANDIDATO'])))

processostodos = pd.read_csv(saida / 'politicos_detalhes.csv', dtype=str)
partestodas = pd.read_csv(saida / 'politicos_partes.csv', dtype=str)

partes0 = partes.copy()
partes0 = partes0.dropna(subset=['nome_normalizado'])
partes0[partes0['nome_normalizado'].str.contains(r'CARLA.*ZAMBELLI.*')]
partes0[partes0['nome_normalizado'].str.contains(r'FERNANDO.*COLLOR.*')]

idx01[idx01['NR_CPF_CANDIDATO'] == '02906287172']

partes[partes['cpf'] == '02906287172']
partes0[partes0['nome_normalizado'].str.contains(r'FERNANDO.*COLLOR.*')]['cpf']



candidatos[candidatos['nome'].str.contains(r'FERNANDO.*COLLOR.*')]['nome'].to_list()

partes = partes.dropna(subset=['nome_normalizado'])
partes[partes['nome_normalizado'].str.contains(r'FERNANDO.*COLLOR.*')]
processostodos[processostodos['numero_cnj'] == '0016341-85.2018.1.00.0000']\
    .to_dict(orient='records')

processostodos[processostodos['numero_cnj'] == '0091206-11.2020.1.00.0000']\
    .to_dict(orient='records')


base02 = pd.read_csv(saida / 'base02_movimentacoes.csv', dtype=str)

for value in base02[base02['numero_cnj'] == '0056748-21.2009.8.19.0038'].itertuples():
    print(value.data_da_movimentacao, value.instancia)

movimentacoes = movs.copy()


movimentacoes = movimentacoes.sort_values(['instancia', 'data_da_movimentacao'], ascending=False)
movimentacoes[
    movimentacoes['numero_cnj'] == '0087386-81.2020.1.00.0000'
][movimentacoes['instancia'] == '2.0']

base01[
    base01['numero_cnj'] == '0002841-54.2015.1.00.0000'
]

processostodos[processostodos['numero_cnj'] == '0016204-06.2018.1.00.0000']\
    .to_dict(orient='records')


base04 = pd.read_csv(saida / 'base04_politicos.csv', dtype=str)
base03 = pd.read_csv(saida / 'base03_partes.csv', dtype=str)

politicos.loc[politicos['NM_CANDIDATO'] == 'VERA RAMOS DA SILVA', 'NR_CPF_CANDIDATO']
base03.loc[base03['nome_da_parte'] == 'VERA RAMOS DA SILVA']
base04.columns

base03.dropna(subset=['nome_da_parte'])[base03.dropna(subset=['nome_da_parte'])['nome_da_parte'].str.contains(r'.*VERA.*RAMOS.*')]

candidatos = pd.read_csv(saida / 'candidatos_info.csv', dtype=str)

set(list(map(len, cpfs)))
list(filter(lambda x: re.search('COLLOR', x), candidatos['nome'].to_list()))

processos = pd.read_csv(saida / 'download_processos.csv', dtype=str)
movs = pd.read_csv(saida / 'download_movimentacoes.csv', dtype=str)

sample = processos[['numero_cnj', 'assunto_extra']]


sample[sample['assunto_extra'].str.contains(r'[Cc]orrupção [Aa]tiva')]
sample[sample['assunto_extra'].str.contains(r'[Cc]orrupção [Pp]assiva')]
sample[sample['assunto_extra'].str.contains(r'[Cc]orrupção [Aa]tiva')]

detalhes = pd.read_csv(saida / 'politicos_detalhes.csv', dtype=str)
reinaldo = pd.read_excel(saida / 'checagens/lote6/lote6_detalhes_checagem_publique_se_karen.xlsx', dtype=str)

detalhes = detalhes[['numero_cnj', 'status_publiquese']]
detalhes = detalhes.drop_duplicates('numero_cnj')
detalhes.rename(columns={'status_publiquese': 'status_antes'}, inplace=True)
reinaldo = reinaldo[['numero_cnj', 'status_publiquese', 'situacao.1']]
reinaldo = reinaldo.drop_duplicates('numero_cnj')
reinaldo.rename(columns={'status_publiquese': 'status_depois'}, inplace=True)

sample = detalhes.merge(reinaldo, on='numero_cnj')
for row in sample.sort_values('numero_cnj').itertuples(name=None, index=False):
    print(
        f'{row[0]:>20}, {row[1]:>3}, {row[2]:>3}, {row[3]:<5}'
    )





#definir método para filtrar os potencias políticos com sobrenomes menos
# comuns
def filtrar_sobrenomes(nome):
    correspondência = set(nome[1]) & sobrenomes_incomuns
    if len(correspondência) > 1:
        correspondência = '|'.join(correspondência)
        correspondência = f'({correspondência})'
    else:
        correspondência = list(correspondência)[0]
    nome_completo = f'^{nome[0]}.*{correspondência}'
    return nome_completo

#definir método para usar em pool apply e extrair nomes dos políticos
def extrair_sobrenomes01(nome):
    return nomesparciais[nomesparciais['nome1'].str.contains(nome)]

#definir método para usar em pool apply e extrair nomes das partes
def extrair_sobrenomes02(nome):
    return origem_nomes[origem_nomes['nome_normalizado'].str.contains(nome)]

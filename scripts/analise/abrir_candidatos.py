from pathlib import Path
import pandas as pd
import re

#definir método para separar assuntos jurídicos
def _split_assunto(textos, ramo_sim):
    texto = re.split(r'\||,(?! )', textos)
    texto = [
        item.upper() if item.upper() in ramo_sim else item for item in texto
    ]
    texto = [item.strip() for item in texto]
    texto = list(filter(None, texto))
    return texto

#definir função para organizar assuntos que são extraídos do digesto
def _organizar_assuntos(texto):
    texto = _split_assunto(texto, main.ramo_sim)
    lista = []
    for i, assunto in enumerate(texto):
        if assunto.isupper() or i == 0:
            lista += [assunto]
        else:
            lista[len(lista)-1] = lista[len(lista)-1] + f' | {assunto}'
    return lista

#definir função para formatar assuntos
def formatar_assuntos(texto):
    assuntos = _organizar_assuntos(texto)
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
    ramos = ' | '.join(ramos)
    assunto_detalhados = ' | '.join(assunto_detalhados)
    return (ramos, assunto_detalhados)

# definir função principal
def main():

    #definir objetos comuns à análise
    entrada = Path('dados/entrada/')
    saida = Path('dados/saida/')
    kwargs = {'low_memory': False, 'dtype': str}

    # importar bases de políticos de 2020
    cand2020 = pd.read_csv(entrada / 'candidatos_eleicoes_2020.csv', **kwargs)

    # filtrar colunas
    colunas = [
        'uf', 'municipio_eleicao', 'cargo', 'nome_completo', 'nome_urna',
        'partido_eleicao', 'cpf'
    ]

    # filtrar candidatos
    cand2020 = cand2020[colunas]

    #carregar bancos de processos
    detalhes = pd.read_csv(entrada / 'politicos_detalhes.csv', **kwargs)
    partes = pd.read_csv(entrada / 'politicos_partes.csv', **kwargs)

    # filtrar partes
    partes = partes[partes['cpf'].notnull()]
    partes = partes.merge(cand2020, on='cpf')

    # # imprimir colunas de interesse
    # for idx, col in enumerate(partes.columns):
    #     print(idx, col)

    # criar lista e filtrar colunas e processos
    colunas = [0, 10, 11, 12, 15, 17, 18, 19, 20, 6, 21, 22]
    candidatos2020_partes = partes.loc[:, partes.columns[colunas]]
    numeros_cnj = set(candidatos2020_partes['numero_cnj'])

    # formatar banco de partes
    candidatos2020_partes.drop_duplicates(['numero_cnj', 'cpf'], inplace=True)
    candidatos2020_partes[['coautor', 'reu']] = (
        candidatos2020_partes[['coautor', 'reu']].applymap(
            lambda x: True if x in ['1.0', 'True'] else False
        )
    )

    # filtrar detalhes
    detalhes = detalhes[detalhes['numero_cnj'].isin(numeros_cnj)]
    detalhes.drop(columns=['numero', 'cpf', 'status_publiquese'], inplace=True)
    detalhes.drop_duplicates(['numero_cnj'], inplace=True)

    #importar tabela processual da união
    tpu = pd.read_excel(entrada / 'identificadores_tpu_avaliado.xlsx')

    #definir conjuntos dos ramos do direito e assuntos jurídicos
    main.ramo_sim = set(tpu.loc[tpu['INTERESSA'] == 'SIM', 'nivel1'])
    ramo_nao = set(tpu.loc[tpu['INTERESSA'] == 'NÃO', 'nivel1']) - main.ramo_sim
    assuntos = set(tpu.loc[tpu['INTERESSA'] == 'SIM', 'ultimo_assunto'])
    args = (assuntos, main.ramo_sim, ramo_nao)

    #criar variável com assuntos:
    assuntos_todos = detalhes.assuntoExtra.to_list()
    assuntos_todos = list(map(formatar_assuntos, assuntos_todos))

    #inserir nos dados
    detalhes['assunto_agrupado'] = [assunto[0] for assunto in assuntos_todos]
    detalhes['assunto_detalhado'] =  [assunto[1] for assunto in assuntos_todos]

    # stats
    print(
        f'Políticos Únicos:\t{len(candidatos2020_partes["cpf"].drop_duplicates())}\nProcessos Únicos:\t{len(detalhes["numero_cnj"])}'
    )

    # salvar
    candidatos2020_partes.to_excel(
        entrada / 'processos_candidatos2020_partes.xlsx', index=False
    )
    detalhes.to_excel(
        entrada / 'processos_candidatos2020_detalhes.xlsx', index=False
    )

# incluir bloco principal de exclusão
if __name__ == '__main__':
    main()



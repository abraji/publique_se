# script para raspar stj
# andre assumpcao: andre.assumpcao@gmail.com
# reinaldo chaves: reichaves@gmail.com

# importar módulos
from tribunais_superiores import Raspador
import argparse, csv, pandas as pd

# definir o bloco de execução principal
def main():

    # alerta de início
    print('\nraspagem iniciada...\n')

    # extrair argumentos passados na linha de comando
    parser = argparse.ArgumentParser()
    parser.add_argument('--inicio', action='store', type=int)
    parser.add_argument('--fim', action='store', type=int)
    args = vars(parser.parse_args())

    # carregar objeto raspador com espera de 30 segundos
    raspador = Raspador(30)

    # carregar lista de políticos
    arqv = 'dados/saida/politicos_2016_2018_100maiorescidades.csv'
    candidatos = pd.read_csv(arqv, dtype=str)

    # selecionar candidatos com foro
    cargos = [
        'GOVERNADOR', '1º SUPLENTE', '2º SUPLENTE', 'DEPUTADO FEDERAL',
        'PRESIDENTE', 'VICE-PRESIDENTE', 'SENADOR'
    ]
    candidatos = candidatos[candidatos['DS_CARGO'].isin(cargos)]
    candidatos.reset_index(drop=True, inplace=True)

    # extrair as variáveis de interesse do banco
    candidatos = candidatos[
        ['nome', 'NR_CPF_CANDIDATO', 'SG_UF', 'NM_MUNICIPIO_NASCIMENTO']
    ]

    # diminuir o banco apenas para os políticos de interesse
    if args:
        inicio, fim = args['inicio'], args['fim']
    else:
        inicio, fim = 0, len(candidatos)

    # selecionar banco
    candidatos = candidatos.loc[inicio : fim,]

    # criar objeto vazio para armazenar dados e contagem
    processos_stf, contagem = [], 0

    # iniciar o loop
    for candidato in candidatos.itertuples(index=False, name=None):

        # contar loop
        contagem += 1

        # imprimir o nome do político
        print(f'{contagem} / {len(candidatos)}. Candidato: {candidato[0]}.')

        try:
            # raspar os dados de um político apenas
            processo_stj = [raspador.stj(*candidato)]

            # adicionar ao banco
            processos_stj += processo_stj

        except:
            raspador = Raspador(30)

    # fechar raspador
    raspador.fechar()

    # incluir saída se o download for vazio
    if not processos_stj: exit()

    # concatenar e salvar
    args = {'ignore_index': True, 'sort': True}
    dados_stj = pd.DataFrame(
        pd.concat([pd.DataFrame(df) for df in processos_stj], **args)
    )

    # mudar nome das colunas
    colunas = {'NÚMERO ÚNICO:': 'num_cnj', 'ÚLTIMA FASE:': 'data'}
    dados_stj.rename(columns=colunas, inplace=True)

    # apagar processos nulos
    dados_stj = dados_stj[dados_stj['num_cnj'].notnull()]

    # salvar em arquivo
    args = {'index': False, 'quoting': csv.QUOTE_ALL, 'encoding': 'utf-8'}
    dados_stj.to_csv(f'dados/saida/processos_stj_{fim}.csv', **args)

    # alerta de finalização
    print('\n...processos salvos.\n')

# executar bloco principal
if __name__ == '__main__':
    main()

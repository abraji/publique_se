# script para raspar stf
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
    entrada = 'dados/entrada/'
    saida = 'dados/saida/'
    arqv = f'{entrada}politicos_2016_2018_100maiorescidades_para_producao.xlsx'
    candidatos = pd.read_excel(arqv)

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
            processo_stf = [raspador.stf(*candidato)]

            # adicionar ao banco
            processos_stf += processo_stf

        except:
            raspador = Raspador(30)

    # fechar raspador
    raspador.fechar()

    # incluir saída se o download for vazio
    if not processos_stf: exit()

    # concatenar e salvar
    args = {'ignore_index': True, 'sort': True}
    dados_stf = pd.concat([pd.DataFrame(pcs) for pcs in processos_stf], **args)

    # mudar nome das colunas
    colunas = {'Número Único': 'num_cnj', 'Data Autuação': 'data'}
    dados_stf.rename(columns=colunas, inplace=True)

    # salvar em arquivo
    args = {'index': False, 'quoting': csv.QUOTE_ALL, 'encoding': 'utf-8'}
    dados_stf.to_csv(f'{saida}processos_stf_{fim}.csv', **args)

    # alerta de finalização
    print('\n...processos salvos.\n')

# executar bloco principal
if __name__ == '__main__': main()

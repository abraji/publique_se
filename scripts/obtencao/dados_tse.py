import os, requests
from tqdm import tqdm

# função para baixar candidatos do tse
def baixar_candidatos(anos=[2014, 2018], destino='dados/entrada/'):

    """ essa função baixa os dados dos candidatos do site do tse """

    # URL base do TSE
    URL = 'http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/'

    # converter a variável local 'anos' para lista
    anos = anos if isinstance(anos, list) else [anos]

    # criar os endereços paras as páginas
    bases = [f'{URL}consulta_cand_{ano}.zip' for ano in anos]

    # criar a pasta de dados se não existir
    if not os.path.isdir(destino):
        os.makedirs(destino)

    # baixar as bases
    for base, ano in tqdm(zip(bases, anos)):
        r = requests.get(base)
        if r.status_code == 200:
            with open(f'{destino}{base[64:]}', 'wb') as file:
                file.write(r.content)
            return f'\nA base de candidatos de {ano} foi baixada com sucesso.'
        else:
            return f'\nA base de candidatos de {ano} não está disponível.'

# executar download se o módulo for carregado sozinho
if __name__ == '__main__': baixar_candidatos()

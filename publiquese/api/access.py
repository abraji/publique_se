

class Digesto:

    """ classe para obtenção dos processos via api Digesto """

    def __init__(self, key):

        # define key de acesso
        self.key = key

        # define parâmetros de autenticaçõa
        self.endpoint = 'https://op.digesto.com.br/api/'
        self.headers  = {'Authorization': 'Bearer ' + key}

        # define parâmetros de cada buscar
        self.processo = self.endpoint + 'tribproc/'
        self.processo_parte = self.endpoint + 'tribprocs/buscar?&'
        self.processo_resumo = self.endpoint + 'tribproc/search_parte_procs'
        self.processo_tabela = self.endpoint + 'tribproc/render_proc_as_report'
        self.tribunalIDs = self.endpoint + 'tribproc_tribunal?per_page=500'
        self.assuntos = self.endpoint + 'tribproc/assuntos'


class JusBrasil(Digesto):

    """ classe para obtenção dos processos via api Dossier Jusbrasil """

    def __init__(self, key):

        # define key de acesso
        self.key = key

        # define parâmetros de autenticaçõa
        self.endpoint = 'https://dossier-api.jusbrasil.com.br'
        self.headers  = {'Authorization': key}

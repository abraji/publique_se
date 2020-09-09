# raspador stj e stf
# andre assumpcao: andre.assumpcao@gmail.com
# reinaldo chaves: reichaves@gmail.com

# importar livrarias necessárias
from selenium import webdriver
import math, re
import pandas as pd

# definir classe para raspar dados
class Raspador:

    """ essa classe contém as funções necessárias para raspar as
        informações do stj e stf com relação aos processos judiciais de
        políticos no brasil.
    """

    # definir constantes (variáveis estáticas)
    STJ = 'https://ww2.stj.jus.br/processo/pesquisa/?aplicacao=processos.ea'
    STF = 'https://portal.stf.jus.br/'

    # definir init método compartilhado por todas as classes
    def __init__(self, wait=60, headless=True, verbose=False, insecure=False):

        # definir configurações do navegador de interesse
        CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
        CHROMEDRIVER = '/usr/local/bin/chromedriver'

        # definir opções do chrome
        chrome_options = webdriver.chrome.options.Options()
        chrome_options.binary_location = CHROME

        # abrir chrome no fundo ou não
        if headless:
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--window-size=1920,1080')

        if insecure:
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--ignore-certificate-errors')

        # armazenar verbose
        self.verbose = verbose

        # abrir o navegador ao fundo
        self.browser = webdriver.Chrome(CHROMEDRIVER, options=chrome_options)

        # carregar espera passada ao browser
        self.browser.implicitly_wait(wait)

    # fechar browser se requisitado
    def fechar(self):
        self.browser.quit()
        if self.verbose:
            print('browser fechado')

    # função interna para raspar cada processo
    def _raspar_stj(self, registro, args):

        # criar condicional para um ou vários resultados
        if registro:

            # passar o número de registro para o xpath e achar seu link
            href = f'//*[text() = "{registro}"]'
            link = self.browser.find_element_by_xpath(href)

            # raspar detalhes do processo
            processo = {'registro': link.text}
            link.click()

        else:
            # achar o número de registro no topo da página de resultados
            reg = 'idSpanNumeroRegistro'
            processo = {'registro': self.browser.find_element_by_id(reg).text}

        # definir busca de detalhes do processo
        detalhes = '//*[@id = "idDivDetalhes"]'

        # criar bloco para capturar erro se não houver detalhes
        try:
            infos = self.browser.find_element_by_xpath(detalhes).text
            infos = infos.split('\n')

            # encontrar informações relevantes nos detalhes
            regex = re.compile(r'(NÚMERO ÚNICO)|(ÚLTIMA FASE)')

            # preencher dicionário
            for i, info in enumerate(infos):
                if re.search(regex, info):
                    if infos[i+1]:
                        processo[info] = infos[i+1]
                    else:
                        processo[info] = None
        except:
            pass

        # voltar para a página anterior
        self.browser.execute_script('window.history.go(-1)')

        # apensar dados do político ao processo
        processo.update(args)

        # retornar informação do processo
        return processo

    # definir raspador do stj
    def stj(self, nome, cpf, uf, municipio_nascimento):

        # salvar argumentos da função em outro objeto
        args = locals()
        args = {k: v for k, v in args.items() if k not in ['self']}

        # acessar a página
        self.browser.get(self.STJ)

        # definir partes a serem clicadas
        partes = ['Autor', 'Reu', 'Outros']
        partes = [f'"parte{parte}Temp"' for parte in partes]
        partes = [f'//*[@name = {parte}]' for parte in partes]

        # definir elementos da página com os quais há interação
        preencher = '//*[@id = "idParteNome"]'
        busca_exata = '//*[@id = "idComboFoneticaPhonosParte_1"]'
        consultar1 = '//*[@id = "idBotaoPesquisarFormularioExtendido"]'

        # clicar em todas partes
        [self.browser.find_element_by_xpath(parte).click() for parte in partes]

        # enviar nome e clicar em 'consultar'
        self.browser.find_element_by_xpath(preencher).send_keys(args['nome'])
        self.browser.find_element_by_xpath(busca_exata).click()
        self.browser.find_element_by_xpath(consultar1).click()

        # definir elementos de interação na segunda página:
        resultados = '//*[@class = "clsMensagemLinha"]'

        # extrair o número de páginas do total de resultados
        candidatos = self.browser.find_element_by_xpath(resultados).text
        candidatos = re.search(r'(?<=em )([0-9])*', candidatos).group(0)
        candidatos = int(candidatos)

        # criar condições para busca com retorno nulo, 1, ou >1
        if candidatos == 0:
            if self.verbose:
                print(f'Não há nenhum processo de: {args["nome"]}.')
            return None
        elif candidatos == 1:
            self.browser.execute_script('quandoClicaPessoa("0")')
        else:
            self.browser.execute_script('quandoClicaMarcarTodos()')
            self.browser.execute_script('quandoClicaPesquisarMarcados()')

        # extrair o número de páginas do total de resultados
        try:
            total = self.browser.find_element_by_xpath(resultados).text
            total = re.search(r'(?<=em )([0-9])*', total).group(0)
            total = int(total)
        except:
            return [self._raspar_stj(None, args)]

        # criar indicador de uma ou mais páginas
        if total < 41:
            pags = range(1)
        else:
            pags = math.ceil(total/40)
            pags = range(pags)
            js = [f'FNavegaProcessosPessoas(true, {n});' for n in pags]

        # criar objeto para armazenar todos os processos
        processos = []

        # iterar a raspagem para todas as páginas
        for pag in pags:

            # passar de página se não for a primeira
            if pag > 0:
                self.browser.execute_script(js[pag])

            # achar todos os links da página
            links = '//*[contains(@href, "/processo/pesquisa/?num_registro")]'
            links = self.browser.find_elements_by_xpath(links)
            registros = [elemento.text for elemento in links]

            # baixar todos os dados de um processo
            processo = [self._raspar_stj(reg, args) for reg in registros]

            # imprimir resultado se usuário pedir verbose
            if self.verbose:
                print(
                    f'Candidato: {args["nome"]}, Processos: {len(processo)}.'
                )

            # apensar processos ao final
            processos += processo

        # chamada de retorno
        return processos

    # função interna para raspar cada processo
    def _raspar_stf(self):
        tag = self.browser.find_element_by_tag_name('table')
        html = tag.get_attribute('outerHTML')
        dados = pd.read_html(html)[0]
        return dados

    # definir raspador do stf
    def stf(self, nome, cpf, uf, municipio_nascimento):

        # salvar argumentos da função em outro objeto
        args = locals()

        # acessar a página
        self.browser.get(self.STF)
        opcao = '//*[@value = "PARTE_OU_ADVOGADO"]'
        texto = '//*[@name = "pesquisaPrincipalParteAdvogado"]'
        botao = '//button[@form = "pesquisa-principal"]'
        self.browser.find_element_by_xpath(opcao).click()
        self.browser.find_element_by_xpath(texto).send_keys(args['nome'])
        self.browser.find_element_by_xpath(botao).click()

        # extrair o número de páginas do total de resultados
        total = '//*[@id = "totalProc"]'
        total = self.browser.find_element_by_xpath(total)
        total = total.get_attribute('value')
        total = int(total)
        if total == 0:
            return None

        # criar indicador de uma ou mais páginas
        if total < 21:
            pags = [1]
        else:
            pags = math.ceil(total/20)
            pags = list(range(1, pags+1))

        # criar objeto para armazenar todos os processos
        processos = []

        # iterar a raspagem para todas as páginas
        for idx, pag in enumerate(pags):

            # baixar todos os dados de um processo
            processos += [self._raspar_stf()]

            # imprimir resultado se usuário pedir verbose
            if self.verbose:
                print(
                    f'Candidato: {args["nome"]}, Processos: {len(processo)}.'
                )

            # passar para a próxima página
            if len(pags) > 1 and len(pags) != idx+1:
                proxPag = '//*[@id= "paginacao-proxima-pagina"]'
                self.browser.find_element_by_xpath(proxPag).click()

        # organizar os dados
        processos = pd.concat(processos, ignore_index=True)
        processos.drop_duplicates(
            'Número Único', ignore_index=True, inplace=True
        )
        nrow = len(processos)
        nomes, cpfs = [args['nome']] * nrow, [args['cpf']] * nrow
        ufs, muns = [args['uf']] * nrow, [args['municipio_nascimento']] * nrow

        # criar colunas para estes novos dados
        processos['nome'], processos['cpf'] = nomes, cpfs
        processos['uf'], processos['municipio_nascimento'] = ufs, muns

        # chamada de retorno
        return processos

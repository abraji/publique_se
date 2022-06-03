#!/usr/bin/env python

"""Tests for `publiquese` package."""

import unittest
from click.testing import CliRunner
from datetime import datetime
from pathlib import Path
from shutil import rmtree

import publiquese
from publiquese import cli


class TestPubliquese(unittest.TestCase):

    """Tests for `publiquese` package."""

    @classmethod
    def setUpClass(self):

        """Set up test fixtures, if any."""

        self.ROOT = Path("tests").resolve()
        self.DATA = self.ROOT / "data"
        self.DATA_RAW = self.ROOT / "data-raw"

        with open(self.ROOT / "api_key.txt", "r") as fp:
            key = fp.readline()
            key = key.replace("\n", "")

        # with open(self.ROOT / "api_token.txt", "r") as fp:
        #     token = fp.readline()
        #     token = token.replace("\n", "")

        self.Digesto = publiquese.Digesto(key)
        # self.Jusbrasil = publiquese.Jusbrasil(token)
        # self.TSE = publiquese.TSE()

    @classmethod
    def tearDownClass(self):

        """Tear down test fixtures, if any."""

        for p in self.DATA.glob("*.csv"):
            p.unlink()
        for p in self.DATA_RAW.glob("*.txt"):
            p.unlink()
        for p in self.DATA_RAW.glob("*.csv"):
            p.unlink()

    def test_000_cli(self):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        assert "publiquese.cli.main" in result.output
        help_result = runner.invoke(cli.main, ["--help"])
        assert help_result.exit_code == 0
        assert "--help  Show this message and exit." in help_result.output

    def test_001_digesto_verificar_api(self):

        r = self.Digesto.testar_api()
        self.assertEqual(r.status_code, 200)

    def test_002_digesto_verificar_baixar_processo(self):

        r = self.Digesto.baixar_processo("399037238")
        self.assertEqual(r.status_code, 200)

    def test_003_digesto_verificar_baixar_processo_com_ids_distintos(self):

        r01 = self.Digesto.baixar_processo("399037238")
        r02 = self.Digesto.baixar_processo("1021887-19.2020.8.26.0100")
        self.assertEqual(r01.json(), r02.json())

    def test_004_digesto_verificar_baixar_processo_fails(self):

        r = self.Digesto.baixar_processo("218207978000")
        self.assertEqual(r.json()["status_op"], "Processo não encontrado")

    def test_005_digesto_verificar_buscar_processos_parte(self):

        r = self.Digesto.buscar_processos_parte(nome_parte="Fernando Holiday")
        self.assertEqual(r.status_code, 200)

    def test_006_digesto_verificar_atualizar_processo(self):

        r = self.Digesto.baixar_processo("1021887-19.2020.8.26.0100")

        data_update = max(r.json()["acessos"])
        data_update = datetime.strptime(data_update, "%Y-%m-%d %H:%M:%S")
        data_diff = datetime.today() - data_update

        if data_diff.days < 4:
            self.assertIsNotNone(r.json()["numero"])
        else:
            r = self.Digesto.atualizar_processo("1021887-19.2020.8.26.0100")
            self.assertEqual(r.status_code, 200)

    def test_007_processar_dados_digesto(self):

        filepath = self.DATA_RAW / "processos_teste.json"
        dados = publiquese.abrir_dados_digesto(filepath)
        self.assertIsNotNone(dados)

    def test_008_extrair_partes_processo(self):

        filepath = self.DATA_RAW / "processos_teste.json"
        dados = publiquese.abrir_dados_digesto(filepath)
        dados_processuais = publiquese.extrair_partes_processo(dados[0])
        for elemento in dados_processuais:
            self.assertGreater(len(elemento.keys()), 0)

    def test_009_designar_keys_advogados(self):

        filepath = self.DATA_RAW / "processos_teste.json"
        dados = publiquese.abrir_dados_digesto(filepath)
        _, _, partes = publiquese.extrair_partes_processo(dados[0])
        advogados = publiquese.designar_keys_advogados(partes)
        self.assertIsNotNone(advogados["advogadoID"])

    def test_010_designar_keys_anexos(self):

        filepath = self.DATA_RAW / "processos_teste.json"
        dados = publiquese.abrir_dados_digesto(filepath)
        capa, _, _ = publiquese.extrair_partes_processo(dados[4])
        anexos = publiquese.designar_keys_anexos(capa)
        self.assertIsNotNone(anexos["anexoID"])

    def test_011_designar_keys_audiencias(self):

        filepath = self.DATA_RAW / "processos_teste.json"
        dados = publiquese.abrir_dados_digesto(filepath)
        capa, _, _ = publiquese.extrair_partes_processo(dados[10])
        audiencias = publiquese.designar_keys_audiencias(capa)
        self.assertIsNotNone(audiencias["data"])

    def test_012_designar_keys_processos_relacionados(self):

        filepath = self.DATA_RAW / "processos_teste.json"
        dados = publiquese.abrir_dados_digesto(filepath)
        capa, _, _ = publiquese.extrair_partes_processo(dados[4])
        processos = publiquese.designar_keys_processos_relacionados(capa)
        self.assertIsNotNone(processos["id"])

    def test_013_descomprimir_arquivos_candidatos_tse(self):

        filepath = self.DATA_RAW / "consulta_cand_AC.zip"
        destpath = self.DATA
        filepaths = publiquese.unzip_candidatos(filepath, destpath)
        filestems = [file.stem for file in filepaths]
        checkfile = [f"consulta_cand_{y}_AC" for y in range(2016, 2021, 2)]
        self.assertEqual(filestems, checkfile)

    def test_014_ler_arquivos_candidatos_tse(self):

        files = [f"consulta_cand_{y}_AC.csv" for y in range(2016, 2021, 2)]
        filepaths = [self.DATA / file for file in files]
        datasets = [publiquese.read_candidatos(fp) for fp in filepaths]
        self.assertEqual([len(dt) for dt in datasets], [2311, 587, 3065])

    def test_015_checar_se_arquivos_candidatos_tem_as_mesmas_colunas(self):

        files = [f"consulta_cand_{y}_AC.csv" for y in range(2016, 2021, 2)]
        filepaths = [self.DATA / file for file in files]
        datasets = [publiquese.read_candidatos(fp) for fp in filepaths]
        column_names = [dt[0] for dt in datasets]
        self.assertTrue(column_names[0] == column_names[1] == column_names[2])

    # def test_007_verificar_criacao_dossier(self):

    #     params = {
    #         "kind": "LAWSUIT",
    #         "artifacts": "lawsuits",
    #         "filter": "Fernando Holiday Silva Bispo",
    #     }

    #     r = self.Jusbrasil.criar_dossier(params=params)
    #     self.assertEqual(r.status_code, 200)
    #     self.assertIsNotNone(r.json())

    # def test_008_verificar_listagem_dossier(self):

    #     r = self.Jusbrasil.listar_dossiers()
    #     self.assertIsNotNone(r.json())

    # def test_009_verificar_execucao_dossier(self):

    #     r = self.Jusbrasil.executar_dossier("andre_assumpcao")
    #     self.assertIsNotNone(r.json())

    # def test_010_verificar_listagem_arquivos_dossier(self):

    #     r = self.Jusbrasil.listar_arquivos_dossier("andre_assumpcao")
    #     self.assertIsNotNone(r.json())

    # def test_011_verificar_download_arquivos_dossier(self):

    #     r = self.Jusbrasil.download_arquivos_dossier("andre_assumpcao")
    #     self.assertIsNotNone(r.json())

    # def test_012_verificar_consulta_processos(self):

    #     r = self.Jusbrasil.consultar_processo("1021887-19.2020.8.26.0100")
    #     self.assertGreater(len(r.json()), 1)
    #     self.assertEqual(r.status_code, 200)

    # def test_007_tse_verificar_api(self):

    #     r = self.TSE.testar_api()
    #     self.assertEqual(r.status_code, 200)

    # def test_008_tse_baixar_candidatos(self):

    #     r = self.TSE.baixar_candidatos(2018)
    #     self.assertRegex(r, "2018")

    # def test_009_tse_baixar_resultados(self):

    #     r = self.TSE.baixar_resultados(2018)
    #     self.assertRegex(r, "2018")

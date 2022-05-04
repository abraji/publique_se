#!/usr/bin/env python

"""Tests for `publiquese` package."""

import unittest
from click.testing import CliRunner
from datetime import datetime
from pathlib import Path

import publiquese
from publiquese import cli


class TestPubliquese(unittest.TestCase):
    """Tests for `publiquese` package."""

    @classmethod
    def setUp(self):
        """Set up test fixtures, if any."""

        ROOT = Path("tests").resolve()
        DATA = ROOT / "data"

        with open(ROOT / "api_key.txt", "r") as fp:
            key = fp.readline()
            key = key.replace("\n", "")

        with open(ROOT / "api_token.txt", "r") as fp:
            token = fp.readline()
            token = token.replace("\n", "")

        self.Digesto = publiquese.Digesto(key)
        # self.Jusbrasil = publiquese.Jusbrasil(token)

    @classmethod
    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_cli(self):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        assert "publiquese.cli.main" in result.output
        help_result = runner.invoke(cli.main, ["--help"])
        assert help_result.exit_code == 0
        assert "--help  Show this message and exit." in help_result.output

    def test_001_verificar_api(self):

        r = self.Digesto.testar_api()
        self.assertEqual(r.status_code, 200)

    def test_002_verificar_baixar_processo(self):

        r = self.Digesto.baixar_processo("399037238")
        self.assertEqual(r.status_code, 200)

    def test003_verificar_baixar_processo_com_ids_distintos(self):

        r01 = self.Digesto.baixar_processo("399037238")
        r02 = self.Digesto.baixar_processo("1021887-19.2020.8.26.0100")
        self.assertEqual(r01.json(), r02.json())

    def test_004_verificar_baixar_processo_fails(self):

        r = self.Digesto.baixar_processo("218207978000")
        self.assertEqual(r.json()["status_op"], "Processo não encontrado")

    def test_005_verificar_buscar_processos_parte(self):

        r = self.Digesto.buscar_processos_parte(nome_parte="Fernando Holiday")
        self.assertEqual(r.status_code, 200)

    def test_006_verificar_atualizar_processo(self):

        r = self.Digesto.baixar_processo("1021887-19.2020.8.26.0100")

        data_update = max(r.json()["acessos"])
        data_update = datetime.strptime(data_update, "%Y-%m-%d %H:%M:%S")
        data_diff = datetime.today() - data_update

        if data_diff.days < 4:
            self.assertIsNotNone(r.json()["numero"])
        else:
            r = self.Digesto.atualizar_processo("1021887-19.2020.8.26.0100")
            self.assertEqual(r.status_code, 200)

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

#!/usr/bin/env python

"""Tests for `publiquese` package."""

import unittest
from click.testing import CliRunner
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

        self.Digesto = publiquese.Digesto(key)

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
        r = self.Digesto.baixar_processo(processoID='218207978')
        self.assertEqual(r.status_code, 200)

    def test_003_verificar_baixar_processo_fails(self):
        r = self.Digesto.baixar_processo(processoID='218207978000')
        self.assertEqual(r.json()['status_op'], 'Processo não encontrado')

    def test_004_verificar_buscar_processos_parte(self):
        r = self.Digesto.buscar_processos_parte(nome_parte='Fernando Holiday')
        self.assertEqual(r.status_code, 200)

    def test_005_verificar_atualizar_processo(self):
        r = self.Digesto.atualizar_processo(processoID='218207978')
        self.assertEqual(r.status_code, 200)

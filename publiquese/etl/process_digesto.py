import json
import re

from collections import defaultdict
from datetime import datetime
from pathlib import Path

from ..globals import *


def abrir_dados_digesto(filepath):

    with open(filepath, "r+") as fp:
        data = json.load(fp)
        return data


def extrair_partes_processo(processo):

    campos = processo.keys()

    if "movs" in campos:

        processo_movs = defaultdict(list)
        for movimento in processo["movs"]:
            for k, v in zip(keys_movs, movimento):
                processo_movs[k].append(v)

        numero_movs = len(processo_movs["data"])
        processo_movs["numero"] = [processo["numero"]] * numero_movs
        processo.pop("movs")

    if "partes" in campos:

        processo_partes = defaultdict(list)
        for parte in processo["partes"]:
            for k, v in zip(keys_partes, parte):
                processo_partes[k].append(v)

        numero_partes = len(processo_partes["parteID"])
        processo_partes["numero"] = [processo["numero"]] * numero_partes
        processo.pop("partes")

    return processo, processo_movs, processo_partes


def designar_keys_advogados(processo):

    if "processosRelacionados" in processo.keys():

        processos_relacionados = defaultdict(list)
        for proc in processo["processosRelacionados"]:
            for k, v in zip(keys_processosRelacionados, proc):
                processos_relacionados[k].append(v)

        numero_procs = len(processos_relacionados["data"])
        processos_relacionados["numero"] = [processo["numero"]] * numero_procs

    return processos_relacionados


def designar_keys_anexos(processo):

    if "anexos" in processo.keys():

        processo_anexos = defaultdict(list)
        for proc in processo["anexos"]:
            for k, v in zip(keys_anexos, proc):
                processo_anexos[k].append(v)

        numero_anexos = len(processo_anexos["data"])
        processo_anexos["numero"] = [processo["numero"]] * numero_anexos

    return processo_anexos


def designar_keys_audiencias(processo):

    if "audiencias" in processo.keys():

        processo_audiencias = defaultdict(list)
        for proc in processo["audiencias"]:
            for k, v in zip(keys_audiencias, proc):
                processo_audiencias[k].append(v)

        numero_audiencias = len(processo_audiencias["data"])
        processo_audiencias["numero"] = [
            processo["numero"]
        ] * numero_audiencias

    return processo_audiencias


# import pandas as pd
# pd.DataFrame(processo_movs[0])

# filepath = DATA_RAW / 'processos_2022-05-11.json'

# dados = abrir_dados_digesto(filepath)
# processo = dados[0]

# dt = [extrair_partes_processo(processo) for processo in dados]

import codecs
import csv
import re
from zipfile import ZipFile
from pathlib import Path, PosixPath


def unzip_candidatos(filepath, destpath):

    """unzip all candidate files into a single folder"""

    filetype = type(filepath)
    filepath = filepath if filetype == PosixPath else Path(filepath)
    destpath = destpath if destpath else Path(".").resolve()

    with ZipFile(filepath) as zipf:
        zipf.extractall(destpath)

    filepaths = Path(destpath).iterdir()
    filepaths = [file for file in filepaths if re.search(r"ulta_", str(file))]

    return sorted(filepaths)


def read_candidatos(filepath):

    """read csv files from candidates in election"""

    with codecs.open(filepath, encoding="cp1252") as fp:
        candidates = csv.reader(fp, delimiter=";", doublequote=True)
        data = [candidate for candidate in candidates]

    return data

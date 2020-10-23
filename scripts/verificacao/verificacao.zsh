#!/bin/zsh
# ativar environment
# conda activate publique-se

# rodar script para baixar dados do formulário do google
python scripts/verificacao/01_downloadSurvey.py
python scripts/verificacao/02_sampleFiles.py --status_publiquese 2 3 4 5 --tamanho 50
python scripts/verificacao/03_uploadFiles.py
python scripts/verificacao/04_sendFiles.py
python scripts/verificacao/05_downloadFiles.py

#!/bin/zsh
# ativar environment
# conda activate publique-se

# rodar script para baixar dados do formulário do google
python scripts/verificacao/01_downloadSurvey.py
python scripts/verificacao/02_sampleFiles.py --status_publiquese 2 3 4 5 --tamanho 50

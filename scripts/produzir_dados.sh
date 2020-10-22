#!/bin/bash
#ativar environment
conda activate publique-se

#obter e limpar dados do tse
python scripts/obtencao/dados_tse.py
python scripts/limpeza/limpar_tse.py

#obter dados dos tribunais superiores
python scripts/obtencao/dados_stf.py &
python scripts/obtencao/dados_stj.py &

#obter dados da receita federal sobre as empresas de políticos
python scripts/obtencao/dados_receitafederal.py &

#usar dados na busca do digesto
python scripts/obtencao/dados_digesto.py
python scripts/obtencao/juntar_digesto.py --tribunais stf stj
python scripts/obtencao/dados_digesto_processosRelacionados.py
python scripts/obtencao/juntar_digesto.py --tribunais extra

#juntar os dados dos tribunais superiores com os processos relacionados
python scripts/limpeza/juntar_todos.py

#obter dados dos tribunais remanescentes
python scripts/obtencao/dados_digesto_outros.py &
python scripts/obtencao/juntar_digesto.py --tribunais outros
python scripts/obtencao/dados_digesto_processosRelacionados_outros.py
python scripts/obtencao/juntar_digesto.py --tribunais outrosextra

#juntar os dados dos tribunais superiores com os processos relacionados
python scripts/limpeza/juntar_todos_outros.py
python scripts/limpeza/juntar_tribunais.py

#limpar os dados para subir na plataforma do publique-se
python scripts/limpeza/filtrar_assuntos.py

# #salvar banco de políticos de 2020
python scripts/analise/abrir_candidatos.py

#analisar os dados
Rscript scripts/analise/nomes_politicos.R
python scripts/limpeza/filtrar_nomesparciais.py
python scripts/limpeza/filtrar_politicos.py

#abrir lotes para verificação
python scripts/analise/selecionar_lotes.py --status_publiquese 3 4 5

#juntar a checagem manual e preparar os arquivos para subir no servidor
python scripts/limpeza/consolidar_partes.py
python scripts/limpeza/limpar_publiquese.py

#rotina para eliminar políticos
python scripts/analise/eliminar_politicos.py

#subir no servidor
source scripts/analise/subir_publiquese.sh




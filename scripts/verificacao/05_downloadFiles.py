# import modules
from io import BytesIO
from pathlib import Path
import concurrent.futures
import json, pickle, re, os
import requests

from googleapiclient.discovery import build
import pandas as pd

# define main function
def main():

    # define constants (==paths)
    ROOT = Path()
    CHECAGEM_01 = ROOT / 'dados/saida/checagens'
    CHECAGEM_02 = ROOT / 'dados/saida/checagens_final'
    SCRIPTS = ROOT / 'scripts'

    # use token with credentials to log into google docs
    with open(SCRIPTS / 'token.pickle', 'rb') as token:
        creds = pickle.load(token)

    # define the ID of the folder
    with open(SCRIPTS / 'drive.json', 'r') as fp:
        drive_ = json.load(fp)
        folder = drive_['folder']

    # build service
    service = build('drive', 'v3', credentials=creds)
    spreadsheet = build('sheets', 'v4', credentials=creds)

    # call the folders API
    root = service.files()
    sheet = spreadsheet.spreadsheets()

    # load list of files online
    participants = pd.read_csv(CHECAGEM_01 / '00_participantes.csv')
    participants = participants['folder'].to_list()

    # filter down files that have been saved
    arqvs = os.listdir(CHECAGEM_02)
    regex = re.compile(r'lote')
    arqvs = sorted(list(filter(regex.search, arqvs)))

    # filter participants list
    participants = [
        participant for participant in participants
        if participant[-7:] not in arqvs
    ]

    # define function to download spreadsheets
    def download_spreadsheets(participant):
        folder = participant.split('/')[-1]
        os.mkdir(CHECAGEM_02 / folder)
        names = [f'{folder}_{file}.xlsx' for file in ['detalhes', 'partes']]
        for name in names:
            query = root.list(q=f'name = "{name}"')
            filelist = query.execute()
            fileID = filelist['files'][0]['id']
            results = sheet.values()
            results = results.get(spreadsheetId=fileID, range='1:1048576')
            results = results.execute()
            data = results['values']
            data = pd.DataFrame(columns=data[0], data=data[1:])
            data.to_excel(CHECAGEM_02 / f'{folder}/{name}', index=False)
        print(f'Download completo: {folder:>15}')

    # download files using multiple threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = executor.map(download_spreadsheets, participants)
        futures = [future.result() for future in futures if future]

# insert execution block
if __name__ == '__main__':
    main()

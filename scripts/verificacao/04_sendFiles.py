# import modules
from pathlib import Path
import json, pickle, re, os

from googleapiclient.discovery import build
from googleapiclient.http import BatchHttpRequest
import pandas as pd

# define callback function
def callback(request_id, response, exception):
    if exception:
        # Handle error
        print(exception)
    else:
        print(f'Permission Id: {response.get("id")}')

# define main function
def main():

    # define constants (==paths)
    ROOT = Path()
    ENTRADA = Path('dados/entrada/')
    SAIDA = Path('dados/saida/')
    CHECAGEM = ROOT / 'dados/saida/checagens'
    SCRIPTS = ROOT / 'scripts'

    # use token with credentials to log into google docs
    with open(SCRIPTS / 'token.pickle', 'rb') as token:
        creds = pickle.load(token)

    # define the ID of the folder
    with open(SCRIPTS / 'drive.json', 'r') as fp:
        drive_ = json.load(fp)
        folder = drive_['folder']

    # load list of users
    participants = pd.read_csv(CHECAGEM / '03_participantes.csv')
    participants = participants[['email', 'folderID']].copy()

    # build service
    service = build('drive', 'v3', credentials=creds)

    # create batch request
    batch = service.new_batch_http_request(callback=callback)

    # loop over participants and share their folders
    for participant in participants.itertuples(index=False):

        # define user permissions on each folder
        user_permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': participant[0]
        }

        # create permissions and send email message
        batch.add(service.permissions().create(
            fileId=participant[1],
            body=user_permission,
            fields='id',
            emailMessage='Publique-se: processos disponíveis para verificação!'
        ))

        # execute
        batch.execute()

        # print message
        print(f'Pasta compartilhada: {participant[0]:>30}')

# insert execution block
if __name__ == '__main__':
    main()

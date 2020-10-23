# import modules
from pathlib import Path
import json, pickle, re, os

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import pandas as pd

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

    # build service
    service = build('drive', 'v3', credentials=creds)

    # call the folders API
    root = service.files()

    # create parameters for search
    mimeType = 'application/vnd.google-apps.folder'
    parents = [folder]

    # carregar lista de checadores
    upload = pd.read_csv(CHECAGEM / '02_participantes.csv')
    participants = upload[['Email Address', 'folder']].copy()

    # create list of folder IDs to download files later
    folderIDs = []

    # loop over participants, create their folders and upload spreadsheets
    for participant in participants.itertuples(index=False):

        #define name
        name = participant.folder

        # create metadata for folder creation
        metadata = {
            'name': name[22:], #participant.folder[22:],
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': parents
        }

        # create folder
        subfolder = root.create(body=metadata).execute()

        # get folder id and upload files to folder
        subfolder_parent = subfolder.get('id')
        folderIDs.append(subfolder_parent)

        # create origin and dest paths and files
        files = ['detalhes', 'partes']
        infiles = [f'{name}/{name[22:]}_{file}.xlsx' for file in files]
        outfiles = [f'{name[22:]}_{file}.xlsx' for file in files]

        # loop over files and upload them
        for infile, outfile in zip(infiles, outfiles):

            # create metadata info for each spreadsheet
            infile_metadata = {
                'name': outfile,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [subfolder_parent]
            }

            # create content for each spreadsheet
            spreadsheet = MediaFileUpload(
                infile, mimetype='application/vnd.google-apps.spreadsheet',
                resumable=True
            )

            # upload spreadsheet
            file = service.files()
            file = file.create(body=infile_metadata, media_body=spreadsheet)
            file = file.execute()

        print(f'Pasta criada: {participant._0:>30}')

    # add folder ID to list of participants
    upload['folderID'] = folderIDs

    participants = pd.read_csv(CHECAGEM / '00_participantes.csv')

    # concatenate and save
    participants = pd.concat([participants, upload])
    participants = participants.dropna(subset=['folderID'])

    # save to disk
    upload.to_csv(CHECAGEM / '03_participantes.csv', index=False, quoting=1)
    participants.to_csv(
        CHECAGEM / '00_participantes.csv', index=False, quoting=1
    )

# insert execution block
if __name__ == '__main__':
    main()

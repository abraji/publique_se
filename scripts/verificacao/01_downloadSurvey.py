from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pathlib import Path
import json, pickle, re, os
import pandas as pd

# define main function
def main():

    # define path variables
    ROOT = Path()
    SCRIPTS = ROOT / 'scripts'
    CHECAGEM = ROOT / 'dados/saida/checagens'

    # define acess scopes
    SCOPES = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/documents'
    ]

    # use token with credentials to log into google docs
    with open(SCRIPTS / 'token.pickle', 'rb') as token:
        creds = pickle.load(token)

    # define the ID of the survey
    with open(SCRIPTS / 'planilha.txt', 'r') as fp:
        survey = fp.readline()[:-1]

    # define scope of responses
    responses = 'Form Responses 1!A:C' #  EDITAR ESTE ESCOPO DE RESPOSTAS

    # build service
    service = build('sheets', 'v4', credentials=creds)

    # call the sheets API
    sheet = service.spreadsheets()

    # get results
    results = sheet.values()
    results = results.get(spreadsheetId=survey, range=responses)
    results = results.execute()

    # build participants list
    data = results['values']
    participantes = pd.DataFrame(columns=data[0], data=data[1:])

    # load and check who has received files
    checadores = pd.read_csv(CHECAGEM / '00_participantes.csv')
    emails = checadores['Email Address'].to_list()
    participantes = participantes[~participantes['Email Address'].isin(emails)]
    checadores = pd.concat([checadores, participantes])

    # save to file
    kwargs = {'index': False, 'quoting': 1}
    checadores.to_csv(CHECAGEM / '00_participantes.csv', **kwargs)
    participantes.to_csv(CHECAGEM / '01_participantes.csv', **kwargs)

# include main execution block
if __name__ == '__main__':
    main()

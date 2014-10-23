import json
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow, flow_from_clientsecrets
from oauth2client.tools import run
import httplib2


def loadCredentials(fileName):
    with open(fileName, 'r') as jsonFile:
        jsonData = jsonFile.read()
        credentialData = json.loads(jsonData)

    installedCredentials = credentialData['installed']

    flow = OAuth2WebServerFlow(
        client_id=installedCredentials['client_id'],
        client_secret=installedCredentials['client_secret'],
        scope='https://www.googleapis.com/auth/bigquery',
        user_agent='sd9-bank/1.0'
    )

    return flow


def authorize(secretJsonFile):
    FLOW = flow_from_clientsecrets(secretJsonFile, 'https://www.googleapis.com/auth/bigquery')

    # If the credentials don't exist or are invalid, run the native client
    # auth flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = Storage('bigquery2.dat')  # Choose a file name to store the credentials.
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = run(FLOW, storage)

    # Create an httplib2.Http object to handle our HTTP requests and authorize it
    # with our good credentials.
    http = httplib2.Http()
    http = credentials.authorize(http)
    return http
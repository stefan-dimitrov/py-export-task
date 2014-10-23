from pprint import pprint
import sys
from googleapiclient.discovery import build
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run
from googleapiclient.errors import HttpError
import httplib2

import json
from datetime import datetime

# FLOW = OAuth2WebServerFlow(
#     client_id='609643013682-32p2r72ob1aopenqc5asqlm7elui4i78.apps.googleusercontent.com',
#     client_secret='twiM1qzqp4aWpGHuFZA6nI9u',
#     scope='https://www.googleapis.com/auth/bigquery',
#     user_agent='sd9-bank/1.0')


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


def loadJobConfig(fileName):
    with open(fileName, 'r') as jsonFile:
        jsonData = jsonFile.read()
        jobConfig = json.loads(jsonData)

    return jobConfig


def exportTable(http, service, jobConfigFile='jobConfig.json'):
    jobConfig = loadJobConfig(jobConfigFile)

    projectId = jobConfig['projectId']
    datasetId = jobConfig['datasetId']
    tableId = jobConfig['tableId']
    bucketName = jobConfig['bucketName']

    url = "https://www.googleapis.com/bigquery/v2/projects/" + projectId + "/jobs"

    jobCollection = service.jobs()
    jobData = {
        'projectId': projectId,
        'configuration': {
            'extract': {
                'sourceTable': {
                    'projectId': projectId,
                    'datasetId': datasetId,
                    'tableId': tableId
                },
                'destinationUris': ['gs://{0}/{1}_{2}.csv'.format(bucketName, tableId, datetime.utcnow())],
            }
        }
    }
    print('Starting job: Exporting {0}.{1} to {2}'
          .format(datasetId, tableId, jobData['configuration']['extract']['destinationUris']))

    insertJob = jobCollection.insert(projectId=projectId, body=jobData).execute()

    import time

    while True:
        status = jobCollection.get(projectId=projectId, jobId=insertJob['jobReference']['jobId']).execute()
        print status
        if 'DONE' == status['status']['state']:
            print "Done exporting!"
            return
        print 'Waiting for export to complete..'
        time.sleep(10)


def main(argv):
    secretJsonFile = argv[1]
    jobConfigFiles = argv[2:]

    FLOW = loadCredentials(secretJsonFile)

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

    service = build('bigquery', 'v2', http=http)

    for jobConfigFile in jobConfigFiles:
        exportTable(http, service, jobConfigFile)


if __name__ == '__main__':
    main(sys.argv)

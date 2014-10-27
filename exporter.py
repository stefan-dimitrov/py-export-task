import sys
from googleapiclient.discovery import build

import json
from datetime import datetime
from authorization import authorizeBQ, authorizeGCS
from gcsDownload import download
import os


def loadJobConfig(fileName):
    with open(fileName, 'r') as jsonFile:
        jsonData = jsonFile.read()
        jobConfig = json.loads(jsonData)

    return jobConfig


def writeGsFilePaths(pathsList, fileName):
    with open(fileName, 'w') as localFile:
        localFile.write('\n'.join(pathsList))


def exportTable(service, projectId, datasetId, tableId, bucketName):

    gsObject = '{0}_{1}_*.csv'.format(tableId, datetime.utcnow())
    gsFilePath = 'gs://{0}/{1}'.format(bucketName, gsObject)

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
                'destinationUris': [gsFilePath],
            }
        }
    }
    print('Starting job: Exporting {0}.{1} to {2}'
          .format(datasetId, tableId, jobData['configuration']['extract']['destinationUris']))

    insertedJob = jobCollection.insert(projectId=projectId, body=jobData).execute()

    import time

    while True:
        status = jobCollection.get(projectId=projectId, jobId=insertedJob['jobReference']['jobId']).execute()
        print status
        if 'DONE' == status['status']['state']:
            print "Done exporting!"
            break
        print 'Waiting for export to complete.'
        time.sleep(10)

    print('Wrote file(s) {0} to cloud storage.'.format(gsFilePath))

    return gsObject


def main(argv):
    secretJsonFile = argv[1]
    jobConfigFile = argv[2]

    jobConfig = loadJobConfig(jobConfigFile)

    http = authorizeGCS(secretJsonFile)
    service = build('storage', 'v1', http=http)

    bucketName = jobConfig['bucketName']


    # gsObjectList = _bqExport(jobConfig, secretJsonFile)
    # _gcsDownload(jobConfig, secretJsonFile, gsObjectList)

    # writeGsFilePaths(gsObjectList, 'exported_files.txt')


def listFilesInBucket(service, bucketName):
    req = service.objects().list(bucket=bucketName,
                                 fields='nextPageToken, items(name)')
    resp = req.execute()

    return [i['name'] for i in resp['items']]


def _gcsDownload(jobConfig, secretJsonFile, gsObjectList):
    http = authorizeGCS(secretJsonFile)
    service = build('storage', 'v1', http=http)

    bucketName = jobConfig['bucketName']
    exportDir = jobConfig['exportDir']

    bucketObjectList = listFilesInBucket(service, bucketName)

    for gsObject in gsObjectList:
        #TODO: resolve wildcards in gsObject name
        download(service, bucketName, gsObject, os.path.join(exportDir, gsObject))


def _bqExport(jobConfig, secretJsonFile):
    http = authorizeBQ(secretJsonFile)

    service = build('bigquery', 'v2', http=http)

    projectId = jobConfig['projectId']
    datasetId = jobConfig['datasetId']
    tableIds = jobConfig['tableIds']
    bucketName = jobConfig['bucketName']

    gsObjectList = []
    for tableId in tableIds:
        gsObjectList.append(exportTable(service, projectId, datasetId, tableId, bucketName))

    return gsObjectList

if __name__ == '__main__':
    # main(sys.argv)
    main(['', 'client_secret.json', 'jobConfig.json'])


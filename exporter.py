import sys
from googleapiclient.discovery import build

import json
from datetime import datetime
from authorization import authorizeBQ


def loadJobConfig(fileName):
    with open(fileName, 'r') as jsonFile:
        jsonData = jsonFile.read()
        jobConfig = json.loads(jsonData)

    return jobConfig


def writeGsFilePaths(pathsList, fileName):
    with open(fileName, 'w') as localFile:
        localFile.write('\n'.join(pathsList))


def exportTable(http, service, jobConfigFile='jobConfig.json'):
    jobConfig = loadJobConfig(jobConfigFile)

    projectId = jobConfig['projectId']
    datasetId = jobConfig['datasetId']
    tableId = jobConfig['tableId']
    bucketName = jobConfig['bucketName']
    gsFilePath = 'gs://{0}/{1}_{2}_*.csv'.format(bucketName, tableId, datetime.utcnow())

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

    return gsFilePath


def main(argv):
    secretJsonFile = argv[1]
    jobConfigFiles = argv[2:]

    http = authorizeBQ(secretJsonFile)

    service = build('bigquery', 'v2', http=http)

    gsFilePathList = []
    for jobConfigFile in jobConfigFiles:
        gsFilePathList.append(exportTable(http, service, jobConfigFile))

    writeGsFilePaths(gsFilePathList, 'exported_files.txt')

if __name__ == '__main__':
    main(sys.argv)


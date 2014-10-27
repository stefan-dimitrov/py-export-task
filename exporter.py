from googleapiclient.discovery import build, Resource

import json
from datetime import datetime
from authorization import authorizeBQ, authorizeGCS
from gcsDownload import download
import os


def loadJobConfig(fileName):
    """
    :type fileName: basestring
    :rtype : dict
    """
    with open(fileName, 'r') as jsonFile:
        jsonData = jsonFile.read()
        jobConfig = json.loads(jsonData)

    return jobConfig


def writeGsFilePaths(pathsList, fileName):
    with open(fileName, 'w') as localFile:
        localFile.write('\n'.join(pathsList))


def exportTable(service, projectId, datasetId, tableId, bucketName):
    """
    :type service: Resource
    :type projectId: basestring
    :type datasetId: basestring
    :type tableId: basestring
    :type bucketName: basestring
    :rtype : basestring
    """
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

    gsObjectList = _bqExport(jobConfig, secretJsonFile)
    _gcsDownload(jobConfig, secretJsonFile, gsObjectList)

    # writeGsFilePaths(gsObjectList, 'exported_files.txt')


def listFilesInBucket(service, bucketName, namePrefix=''):
    """
    :type service: Resource
    :type bucketName: basestring
    :type namePrefix: basestring
    :rtype : list
    """
    req = service.objects().list(bucket=bucketName,
                                 prefix=namePrefix,
                                 fields='nextPageToken, items(name)')
    resp = req.execute()

    if not resp:
        return []

    return [i['name'] for i in resp['items']]


def resolveWildcardGsObject(objectName, service, bucketName):
    """
    :type objectName: basestring
    :type service: Resource
    :type bucketName : basestring
    :rtype : list
    """

    resolvedList = []

    wildcardIndex = objectName.find('*', 0)
    if wildcardIndex > 0:
        resolvedList = listFilesInBucket(service, bucketName, objectName[:wildcardIndex])

    else:
        resolvedList.append(objectName)

    return resolvedList


def _gcsDownload(jobConfig, secretJsonFile, gsObjectList):
    """
    :type jobConfig: dict
    :type secretJsonFile: basestring
    :type gsObjectList: list
    """
    http = authorizeGCS(secretJsonFile)
    service = build('storage', 'v1', http=http)

    bucketName = jobConfig['bucketName']
    exportDir = jobConfig['exportDir']

    os.makedirs(exportDir)

    resolvedGsObjectList = []
    for rawGsObject in gsObjectList:
        resolvedGsObjectList.extend(resolveWildcardGsObject(rawGsObject, service, bucketName))

    for gsObject in resolvedGsObjectList:
        download(service, bucketName, gsObject, os.path.join(exportDir, gsObject))


def _bqExport(jobConfig, secretJsonFile):
    """
    :type jobConfig: dict
    :type secretJsonFile: basestring
    :rtype : list
    """
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


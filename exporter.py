import sys
from ftplib import FTP_TLS
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


def gcsDownload(jobConfig, secretJsonFile, gsObjectList):
    """
    :type jobConfig: dict
    :type secretJsonFile: basestring
    :type gsObjectList: list
    :rtype : list
    """
    http = authorizeGCS(secretJsonFile)
    service = build('storage', 'v1', http=http)

    bucketName = jobConfig['bucketName']
    exportDir = jobConfig['exportDir']

    if not os.path.exists(exportDir):
        os.makedirs(exportDir)

    resolvedGsObjectList = []
    for rawGsObject in gsObjectList:
        resolvedGsObjectList.extend(resolveWildcardGsObject(rawGsObject, service, bucketName))

    localFileList = []
    for gsObject in resolvedGsObjectList:
        localFilePath = os.path.join(exportDir, gsObject)
        download(service, bucketName, gsObject, localFilePath)
        localFileList.append(localFilePath)

    return localFileList


def bqExport(jobConfig, secretJsonFile):
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


def uploadToFtp(fileList, remoteDir, host, user, password):
    """
    :type fileList: list
    :type remoteDir: basestring
    :type host: basestring
    :type user: basestring
    :type password: basestring
    """

    ftps = FTP_TLS(host)
    ftps.sendcmd('USER %s' % user)
    ftps.sendcmd('PASS %s' % password)

    for fileItem in fileList:
        fileName = os.path.split(fileItem)[1]
        remoteFilePath = os.path.join(remoteDir, fileName)

        print('Uploading file [{0}] to ftp at [{1}]'.format(fileName, remoteFilePath))
        ftps.storlines('STOR {0}'.format(remoteFilePath), open(fileItem))
        print('Done.')

    ftps.quit()


def main(argv):
    secretJsonFile = argv[0]
    jobConfigFile = argv[1]

    jobConfig = loadJobConfig(jobConfigFile)

    gsObjectList = bqExport(jobConfig, secretJsonFile)
    localFileList = gcsDownload(jobConfig, secretJsonFile, gsObjectList)

    ftpHost = jobConfig['ftpHost']
    ftpUser = jobConfig['ftpUser']
    ftpPassword = jobConfig['ftpPassword']
    ftpDir = jobConfig['ftpDir']

    print('Opening ftp connection ({0})'.format(ftpHost))
    uploadToFtp(localFileList, ftpDir, ftpHost, ftpUser, ftpPassword)
    print('Export job complete.')


if __name__ == '__main__':
    main(sys.argv[1:])


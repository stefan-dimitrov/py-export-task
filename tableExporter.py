import sys
import os
from pprint import pprint
from authorization import authorizeBQ
from datetime import datetime
import json
from googleapiclient.discovery import build

from ftplib import FTP_TLS


def loadJobConfig(fileName):
    with open(fileName, 'r') as jsonFile:
        jsonData = jsonFile.read()
        jobConfig = json.loads(jsonData)

    return jobConfig


def uploadToFtp(fileList, remoteDir, host, user, password):
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
    secretsJsonFile = argv[1]
    jobConfigFile = argv[2]

    jobConfig = loadJobConfig(jobConfigFile)

    http = authorizeBQ(secretsJsonFile)
    service = build('bigquery', 'v2', http=http)

    projectId = jobConfig['projectId']
    datasetId = jobConfig['datasetId']
    exportDir = jobConfig['exportDir']

    ftpHost = jobConfig['ftpHost']
    ftpUser = jobConfig['ftpUser']
    ftpPassword = jobConfig['ftpPassword']
    ftpDir = jobConfig['ftpDir']

    fileList = []

    print('Opening ftp connection ({0})'.format(ftpHost))
    uploadToFtp(fileList, ftpDir, ftpHost, ftpUser, ftpPassword)
    print('Export job complete.')

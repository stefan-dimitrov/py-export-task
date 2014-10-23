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


def exportTable(service, projectId, datasetId, tableId):

    queryData = {'query': 'SELECT * FROM [{0}:{1}.{2}];'.format(projectId, datasetId, tableId)}

    jobCollection = service.jobs()
    queryResponse = jobCollection.query(projectId=projectId, body=queryData).execute()

    rowDataList = [[]]

    # Extract column names
    for field in queryResponse['schema']['fields']:
        rowDataList[0].append(field['name'])

    # Extract row data
    for row in queryResponse['rows']:
        resultRow = []
        for field in row['f']:
            resultRow.append(field['v'])

        rowDataList.append(resultRow)

    return rowDataList


def writeTableToCSV(tableId, rowList, directoryPath, delimiter=','):
    if not os.path.exists(directoryPath):
        os.makedirs(directoryPath)

    fileName = '{0}_{1}.csv'.format(tableId, datetime.now())
    fullFilePath = os.path.join(directoryPath, fileName)

    with open(fullFilePath, 'w') as csvFile:
        lines = ['%s\n' % (delimiter.join(i)) for i in rowList]
        csvFile.writelines(lines)

    return fullFilePath


def uploadToFtp(fileList, remoteDir, host, user, password):
    ftps = FTP_TLS(host)
    ftps.sendcmd('USER %s' % user)
    ftps.sendcmd('PASS %s' % password)

    for fileItem in fileList:
        fileName = os.path.split(fileItem)[1]
        ftps.storlines('STOR {0}'.format(os.path.join(remoteDir, fileName)), open(fileItem))

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

    for tableId in jobConfig['tableIds']:
        rowList = exportTable(service, projectId, datasetId, tableId)
        fileList.append(writeTableToCSV(tableId, rowList, exportDir))

    uploadToFtp(fileList, ftpDir, ftpHost, ftpUser, ftpPassword)


if __name__ == '__main__':
    main(sys.argv)
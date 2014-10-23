import sys
import os
from pprint import pprint
from authorization import authorize
from datetime import datetime
import json
from googleapiclient.discovery import build


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


def main(argv):
    http = authorize('client_secret.json')
    service = build('bigquery', 'v2', http=http)

    rowList = exportTable(service, 'sd9-bank', 'sd9dataset', 'Accounts')
    writeTableToCSV('Accounts', rowList, 'results/')


if __name__ == '__main__':
    main(sys.argv)
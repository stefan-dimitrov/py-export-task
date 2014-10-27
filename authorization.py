from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow, flow_from_clientsecrets
from oauth2client.tools import run
import httplib2


def authorizeBQ(secretJsonFile):
    """
    :type secretJsonFile: basestring
    :rtype : httplib2.Http
    """
    FLOW = flow_from_clientsecrets(secretJsonFile, scope='https://www.googleapis.com/auth/bigquery')

    # If the credentials don't exist or are invalid, run the native client
    # auth flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storageFile = 'bigquery2.dat'

    http = _authorizeCredentials(FLOW, storageFile)
    return http


def authorizeGCS(secretJsonFile):
    """
    :type secretJsonFile: basestring
    :rtype : httplib2.Http
    """
    FLOW = flow_from_clientsecrets(secretJsonFile, scope='https://www.googleapis.com/auth/devstorage.read_only')

    # If the credentials don't exist or are invalid, run the native client
    # auth flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storageFile = 'gcs.dat'

    http = _authorizeCredentials(FLOW, storageFile)
    return http


def _authorizeCredentials(FLOW, storageFile):
    """
    :type FLOW: OAuth2WebServerFlow
    :type storageFile: basestring
    :rtype : httplib2.Http
    """
    storage = Storage(storageFile)  # Choose a file name to store the credentials.
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = run(FLOW, storage)

    # Create an httplib2.Http object to handle our HTTP requests and authorize it
    # with our good credentials.
    http = httplib2.Http()
    http = credentials.authorize(http)
    return http
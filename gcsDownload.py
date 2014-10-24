import httplib2
import os
import random
import sys
import time

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build as discovery_build

from authorization import authorizeGCS

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

# Number of times to retry failed downloads.
NUM_RETRIES = 5

# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * 1024 * 1024

# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'


def print_with_carriage_return(s):
    sys.stdout.write('\r' + s)
    sys.stdout.flush()


def download(service, bucketName, objectName, filename):

    print 'Building download request...'
    f = file(filename, 'w')
    request = service.objects().get_media(bucket=bucketName,
                                          object=objectName)
    media = MediaIoBaseDownload(f, request, chunksize=CHUNKSIZE)

    print 'Downloading bucket: %s object: %s to file: %s' % (bucketName,
                                                             objectName,
                                                             filename)

    progressless_iters = 0
    done = False
    while not done:
        error = None
        try:
            progress, done = media.next_chunk()
            if progress:
                print_with_carriage_return(
                    'Download %d%%.' % int(progress.progress() * 100))
        except HttpError, err:
            error = err
            if err.resp.status < 500:
                raise
        except RETRYABLE_ERRORS, err:
            error = err

        if error:
            progressless_iters += 1
            handle_progressless_iter(error, progressless_iters)
        else:
            progressless_iters = 0

    print '\nDownload complete!'


def handle_progressless_iter(error, progressless_iters):
    if progressless_iters > NUM_RETRIES:
        print 'Failed to make progress for too many consecutive iterations.'
        raise error

    sleeptime = random.random() * (2 ** progressless_iters)
    print ('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
           % (str(error), sleeptime, progressless_iters))
    time.sleep(sleeptime)


def main():
    http = authorizeGCS('client_secret.json')
    service = discovery_build('storage', 'v1', http=http)

    download(service, 'sd9-bank.appspot.com', 'file-for-test.txt', 'local-for-test.txt')


if __name__ == '__main__':
    main()
import os
import sys
import getopt
import boto
import uuid
import time
import tarfile
import shutil
import math
import csv
import traceback
from filechunkio import FileChunkIO
from datetime import datetime, timedelta
from os.path import splitext, join
from os import walk
from os import path
from pprint import pprint

#Process files from a given bucket
def getFilesFromBucket(fWriter):
        global manBucket, breakManRun

	#alternative method
	#access_key = '<YOUR_KEY>'
	#secret_key = '<YOUR_SECRET_KEY>'
        #conn = boto.connect_s3(
        #        aws_access_key_id = access_key,
        #        aws_secret_access_key = secret_key
        #        )
	conn = boto.connect_s3()

	#Connect to our desired Bucket
        s3 = conn.get_bucket(manBucket)

	#Fetch individual objects from the bucket, "folders", files etc... 
	#We are after current versions here.
	print "Bucket key | Version ID"
	for manKey in s3.list_versions():
		try:
                        #little elaborate, last minute... just a means to avoid trying to process delete_marker entries
                        canProcessRecord = True
                        if(hasattr(manKey, 'DeleteMarker')):
                            canProcessRecord = False
                        if(hasattr(manKey, 'delete_marker')):
                            if(manKey.delete_marker == True):
                                canProcessRecord = False
			if(manKey.is_latest and canProcessRecord == True):
                                if breakManRun:
                                    debugPrintKey(manKey)
                                    sys.exit(1)

				print str(manKey.path) + manKey.name + " | " + str(manKey.version_id)
				if manKey.path is None:
					fullKey = manKey.name
				else: 
					fullKey = manKey.path + manKey.name

				fWriter.writerow([fullKey,str(manKey.version_id)])

		except:
			print "If we hit an exception we should close and remove the manifest as it will likely be corrupt or at least incomplete."
			print "Something wrong with: " + manKey.name
			print "Unexpected Error: ", sys.exc_info()[0]
                        traceback.print_exc(file=sys.stdout)
                        debugPrintKey(manKey)
			breakManRun = True
                        sys.exit(1)


#Debug method to see what the heck is in the object we are looking at
def debugPrintKey(keyObj):
	pprint(vars(keyObj))

#Desired Action: [write-manifest, download-by-manifest]
manAction = "write-manifest"

#Bucket Name
manBucket = ""

#Manifest Name
manFile = ""

#Global for managing manifest list
manList = []

#Flag to handle connection and listing exceptions to avoid writing or reading bad manifests
breakManRun = False

#setup Usage handler 
def main(argv):
    global manFile, manBucket
    try:
        opts, args = getopt.getopt(argv, "hm:b:")
    except getopt.GetoptError:
        printHelp()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h","help","--help"):
            printHelp()
            sys.exit(2)
        elif opt == '-b':
            manBucket = arg
        elif opt == '-m':
            manFile = arg
    if not manFile:
        printHelp()
        sys.exit(2)
    if not manAction:
        printHelp()
        sys.exit(2)
    if not manBucket:
        printHelp()
        sys.exit(2)

#argv help for execution
def printHelp():
    print 's3_manifest.py -m <manifest output filename> -b <AWS bucket name>\n'

#n usage handler on main program init
if __name__ == "__main__":
    main(sys.argv[1:])
    if manAction == "write-manifest":
	#open csvWriter
	with open(manFile, 'wb') as csvFH:
		csvWriter = csv.writer(csvFH, delimiter=',', quotechar='\\', quoting=csv.QUOTE_MINIMAL)

		#create csv from current bucket
		getFilesFromBucket(csvWriter)


#Set the current date for use in writing manifest files
checkdateCurr = datetime.now()

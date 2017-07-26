#!/usr/bin/python

import threading
import time
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
import hashlib
from filechunkio import FileChunkIO
from datetime import datetime, timedelta
from os.path import splitext, join
from os import walk
from os import path
from pprint import pprint

# This program was designed as a simple multi-threaded download manager for AWS
# to take a manifest of files generated from a versioned s3 bucket and download (mirror) the contents and directory structure 
# locally at some file root "/ or /myawsfiles/ or whatever you want to pass in"
# thread count and files managed by thread are configurable parameters as described in help 
# Execution sequence:
#   Process args and set vars
#   Initialize a new ThreadManager
#   Identify and open csv
#       - iterate through the object for the next "filesPerThread" count entries, updating the chunkFiles list with a awsObject entry
#           - Once filesPerThread has matched the number of entries in chunkFiles.size (or when execution has finished but we have a number of pending downloads ready) 
#               see if we can launch a new thread or if we have exceeded our thread count by checking the ThreadManager boolean
#               - If we can then add a new thread with the version and filepath/filename to ThreadManager
#                   - Set our filesPerThread counter back to 0 to start loading a file chunk for the next thread
#                   - In executing thread:
#                       - iterate through chunkFiles list passed in to thread
#                           - break apart passed in path/filename into path:filename
#                           - create local directories from the args fileRoot setting above
#                           - try: (to avoid deleted handles breaking things) attempt to get the object by its version id to that location
#                           - increment files processed successfully counter for thread
#                       - When finished with chunk print number of files processed successfully and any that weren't
#                       - When finished exit thread to free up room for a new one.
#               - If we cannot then sleep the main execution loop while the threads run and close out in the background
#           - If we have no more entries check to see if there is a size greater than 0 in the chunkFiles list, in which case spawn a thread and send it to the manager to finish
#       - close all threads
#       - close csv
#       - exit printing number of objects processed

#define help / usage text
def printHelp():
    print '\ns3_get_versioned_files_by_manifest.py -m <manifestfile> -r <fileroot Default: /opt/> -b <aws bucket name>'
    print ' Optional args: '
    print ' -t <max handling threads. Default: 10> '
    print ' -f <files to process per thread. Default: 100>\n'

#Main processing function. This is what starts the whole thing.
def processManifest():
    totalFilesProcessed = 0
    with open(manifestFile, 'rb') as csvfile:
        csvFH = csv.reader(csvfile, delimiter=',',quotechar='\\', quoting=csv.QUOTE_MINIMAL)
        debugPrintKey("File opened by reader", 3)
        debugPrintKey(csvFH, 3)
        filesChunked = []
        #read each row in the csv
        for row in csvFH:
            if(len(row) > 0):
                totalFilesProcessed += 1
                fNameAndPath = row[0] 
                fVersion = row[1]
                #split each line into an awsEntry object (defined below) to hold the components and provide functions like local path creation
                filesChunked.append(awsEntry(fNameAndPath,fVersion))

                #if totalFilesProcessed % filesPerThread then compute the percentage remaining... possible? Would need to know the bucket key count first.
		#totally possible, we have the manifest. Its just the number of lines in the file as the entries being processed. 

                #Ok, we have reached out required chunk size (also need way to handle "last" chunk size... send to the ThreadPool Manager for processing
                #this could get well ahead of the number currently running... we might want to block if there is a large buildup in the pool backlog
                if(len(filesChunked) >= filesPerThread):
                    debugPrintKey("Files per thread threshold met. Executing new thread with chunk",1)
                    PoolMan.addThreadToPool(filesChunked)        
                    #reset filesChunked so we dont redownload stuff in another thread.
                    filesChunked = []
                
        #execute the last chunk if there is one (that was less than the required minimum set above for size)... partial chunk basically
        if(len(filesChunked) > 0):
            debugPrintKey("Finished csv without meeting file threshold for new thread. Executing new thread to finish",1)
            PoolMan.addThreadToPool(filesChunked)        

#Simple class to manage the elements of the files properties... local destination and folder structure, remote key, version etc...
class awsEntry ():
    def __init__(self, fName, version):
        self.path, self.name = os.path.split(fName)
        self.version = version
        self.awsKey = fName
        debugPrintKey("awsEntry Created with data:",4)
        debugPrintKey(self,4)
    def getLocalPath():
        global fileRoot
        return fileRoot + self.awsKey

#Singleton(ish) to track and manage our thread pool
class ThreadManager:
    class __ThreadManager:
        #Note entirely sure this is working the way I want it to. The goal 
        #is to enable an async behavior that will allow us to severly limit the polling we need to do
        #and leverage an async behavior (so as not to waste any ms waiting

        def __init__(self):
            self.myThreadPool = {}
            self.currentThreadCount = 0
            self.pendingThreads = []

        def addThreadToPool(self, filesChunk):
            global threadCountMax
            self.currentThreadCount += 1
            threadName = "threadid-" + str(self.currentThreadCount)

            #spawn a thread and pass it a chunk of the manifest files
            #if all threads are busy dont continue piling on work, just queue it in the background
            #while(self.isThreadPoolFull()):
            #    debugPrintKey("Thread pool is full. Holding off on launching new threads until load drops down. ", 1)
            #    time.sleep(1)

            #thread pool has freed up, run new thread
            debugPrintKey("Processing time available, launching new thread: " + threadName, 1)
            debugPrintKey("New thread launching with file chunk: ", 3)
            debugPrintKey(filesChunk, 3)
            newThread = FileChunkingThread(self.currentThreadCount, threadName, filesChunk)
            if not self.isThreadPoolFull():
                newThread.start()
            else:
                self.pendingThreads.append(newThread)            

            #add thread to a tracking dictionary
            self.myThreadPool[threadName] = newThread   

        def runSleepingThreads(self):
            #we arrived here because a thread called home (and therefore is no longer busy)... pull a worker off the sleeping heap and
            #set them to task
            if(len(self.pendingThreads) > 0):
                getAThread = self.pendingThreads.pop()
                getAThread.start()

        def getThreadPool(self):
            return self.myThreadPool

        def isThreadPoolFull(self):
            global threadCountMax
            if(threading.activeCount() >= threadCountMax):
                debugPrintKey("Threading active count(boolean check against threadCountMax): " + str(threading.activeCount) + " : " + str(threadCountMax),3)
                return True
            else:
                return False

        def isThreadPoolBusy(self):
            if(threading.activeCount() > 0):
                return True
            else:
                return False

        def closeThread(self,threadName):
            if self.myThreadPool.get(threadName):
                debugPrintKey("Closing thread: " + threadName, 2)
                self.myThreadPool.pop(threadName)
                self.runSleepingThreads()
        
        #we aren't doing anything to merit this yet.
        def closeThreadPool(self):
            for threadID,aThread in self.myThreadPool():
                aThread.join()

    #storage for the instance reference
    __instance = None

    #logic to make this a simple singleton(ish)
    def __init__(self):
        if not ThreadManager.__instance:
            ThreadManager.__instance = ThreadManager.__ThreadManager()

    def __getattr__(self, name):
        debugPrintKey("Setting attr " + name + " for inner singleton...ish", 5)
        debugPrintKey(self.__instance, 5)
        return getattr(self.__instance, name)

#create a thread to perform a chunk of work
class FileChunkingThread (threading.Thread):

    #While initializing setup our s3 credentials and bucket connection
    #this means there can be <threads>*<aws connections> active concurrently
    def __init__(self, threadID, name, filesChunk):
        global awsBucketName, fileRoot, debugLvl
        threading.Thread.__init__(self)

        #thread identifiers 
        self.threadID = threadID
        self.name = name

        #vitally important, this is the list of files this thread is about to process
        debugPrintKey("Incoming file chunk for thread: ", 3)
        debugPrintKey(filesChunk, 3)
        self.filesChunk = filesChunk

        #handy if we need to debug the AWS connection
        if debugLvl > 2:
            boto.set_stream_logger('boto')

        #connect to AWS
        self.conn = boto.connect_s3()
        self.s3 = self.conn.get_bucket(awsBucketName)
        debugPrintKey("s3 connection? ", 5)
        debugPrintKey(self.s3, 5)

        #setup tracking data for things like progress reports
        self.processedFiles = 0
        self.successFiles = 0

    def run(self):
        global PoolMan
        print "Running Thread for chunk"
        for aFile in self.filesChunk:
            #capture basic metrics, success will be removed if error is raised
            self.processedFiles += 1
            self.successFiles += 1
            try:
                debugPrintKey("awsEntry file info: ", 2)
                debugPrintKey(aFile, 2)
                #Create the local path structure on the hard drive
                self.createLocalFilePath(aFile.awsKey)            
        
                #download the file by its version
                self.downloadAWSFileToLocal(aFile)
            except:
                #handle exceptions and record some stats without dying entirely, could be switchable behavior
                self.successFiles -= 1
                debugPrintKey("Unexpected Error while trying to download AWS file: " + str(sys.exc_info()[0]), 0)

        #provide some information on how we did to the user
        print("Thread: " + self.name + " exited. Processed: " + str(self.successFiles) + " out of " + str(self.processedFiles))

        #we are ready for more work, let the thread pool manager know
        PoolMan.closeThread(self.name)

    #download a file version locally
    def downloadAWSFileToLocal(self, awsObj):
        global awsBucketName, fileRoot
        debugPrintKey("Fetching AWS Key: " + awsObj.awsKey, 1)
        debugPrintKey(awsObj, 2)

        k = self.s3.get_key(awsObj.awsKey)
        debugPrintKey("AWS Reported Key meta to be: ", 3)
        debugPrintKey(k, 3)

        #check to see if file exists locally already. If it does see if the md5 is a match.
        #if the md5 matches the key there is no point in downloading the same file again
        #NOTE: this may well not work with files larger than memory or if the AWS etag does not represent the entirety of the md5 for the file
        newLocalDst = fileRoot + awsObj.awsKey
        debugPrintKey("Copying Key to : ", 1)
        debugPrintKey(newLocalDst, 1)
        if(os.path.exists(newLocalDst)):
            debugPrintKey("Local path already exists for key: " + newLocalDst, 1)
            thisFileMD5 = self.md5sum(newLocalDst)
            if( thisFileMD5 == k.etag.strip('"') ):
                debugPrintKey("Local key contains a matching md5sum to the s3 download, skipping.", 1)
                debugPrintKey("MD5 matches: " + thisFileMD5 + " : " + k.etag, 2)
            else:
                debugPrintKey("MD5 was not a match: " + thisFileMD5 + " : " + k.etag, 1)
                k.get_contents_to_filename(newLocalDst, version_id=awsObj.version)
                debugPrintKey("Retrieved key: ", 1)
                debugPrintKey(k, 1)
        else:
            debugPrintKey("Downloading new file: " + newLocalDst + " - version: " + awsObj.version, 1)
            k.get_contents_to_filename(newLocalDst, version_id=awsObj.version)

    #adding efficient and fast md5 processing function from stack overflow user: rsandwick3
    #https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
    def md5sum(self, filename, blocksize=65536):
        hash = hashlib.md5()
        with open(filename, "rb") as f:
            for block in iter(lambda: f.read(blocksize), b""):
                hash.update(block)
        return hash.hexdigest()

    #setup simple function for creating the directory path that adds the directories to the defined file root (be careful with this, naming poorly could seriously overwrite 
    #your base OS files
    def createLocalFilePath(self, fName):
        global fileRoot
        #break apart file by "/" to establish the disk path we want to save it to respectively
        path, name = os.path.split(fName)
        try:
            fullPath = fileRoot + "/" + path
            #check to see if directory path already exists, if it does don't remake
            if not os.path.isdir(fullPath):
                os.makedirs(fullPath)
                debugPrintKey("Creating disk path: " + fullPath, 1)
        except:
            debugPrintKey("Unexpected Error while trying to create local directory structure: " + sys.exc_info()[0], 0)

#quick adaptable log function to give us more detailed messages as we decide based on global debugLvl
def debugPrintKey(keyObj, logLvl):
    global debugLvl
    if( logLvl <= debugLvl ): 
        if(type(keyObj) is str):
            pprint(keyObj)
        elif(type(keyObj).__name__ == 'instance'):
            pprint(vars(keyObj))
        elif(isinstance(keyObj,dict)):
            pprint(vars(keyObj))
        elif(keyObj.__class__ == 'dict'):
            pprint(vars(keyObj))
        elif(type(keyObj) is list):
            pprint(str(keyObj))
        elif(type(keyObj) is tuple):
            pprint(str(keyObj))
        else:
            pprint(keyObj)

#Main program debug level    
#generally this means: 0 all is well, 1 - I want to see files and progress, 2 - gimme transfer deets, 3 - seriously... lemme know everything
debugLvl = 0

#what is the AWS manifest name?
manifestFile = ""

#where do we want to establish the file hierarchy root?
fileRoot = "/opt/"

#Max concurrent threads executing before we wait
threadCountMax = 10

#how many files do we add to the processing chunk for each thread?
filesPerThread = 100

#AWS Bucket name
awsBucketName = ""

#ThreadPoolManager container (is a singleton, but this makes it easy to pass around between threads)
PoolMan = ThreadManager()

#setup Usage handler 
def main(argv):
    global fileRoot, manifestFile, awsBucketName
    try:
        #opts that require an argument should be followed by a colon, long options that require an arg should be followed by an = sign
        opts, args = getopt.getopt(argv, "hm:b:r:t:f:", ["help"])
    except getopt.GetoptError:
        printHelp()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h","help","--help"):
            printHelp()
            sys.exit(2)
        if opt == '-m':
            manifestFile = arg
        elif opt == '-b':
            awsBucketName = arg
        elif opt == '-r':
            #check to see if we have a trailing / on the fileRoot arg, if not add it. Oh and MAKE SURE its not empty first, otherwise you are describing root...
            possibleRoot = str(arg)
            if(len(possibleRoot.strip(',-.\n\r\t ')) > 2):
                if not possibleRoot.endswith("/"):
                    fileRoot = possibleRoot + "/"
                else:
                    fileRoot = possibleRoot
        elif opt == '-t':
            threadCountMax = arg
        elif opt == 'f':
            filesPerThread = arg  
    if not manifestFile:
        printHelp()
        sys.exit(2)
    if not fileRoot:
        printHelp()
        sys.exit(2)
    if not awsBucketName:
        printHelp()
        sys.exit(2)

#Run usage handler on main program init
if __name__ == "__main__":
    main(sys.argv[1:])
    debugPrintKey("\nmanifestFile: " + manifestFile + "\nawsBucketName: " + awsBucketName + "\nfileRoot: " + fileRoot + "\nthreadCountMax: " + str(threadCountMax) + "\nfilesPerThread: " + str(filesPerThread),1)
    processManifest()






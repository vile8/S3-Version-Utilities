# S3-Version-Utilities
Simple Utilities to work with S3 Versioned buckets. 

I have created 2 utilities to help manage files in versioned s3 buckets.
One creates a manifest from the current bucket, the other syncs down 
the specific versions in the manifest to a configurable folder. 

### AWS Server Setup ###
You will need a versioned S3 bucket 

### AWS User Credentials ###
You will need a IAM user with appropriate permissions to read the s3 bucket objects. 
Then put the user credentials on the machine you expect the script to run on 
under the ~/.aws/credentials file for the user it will be running as 
in the format:

[default]
aws_access_key_id = <your users aws key id>
aws_secret_access_key = <your users aws secret id>

### Local Setup for Python ###
We use the Python BOTO toolkit. You will need to pip install all the libs 
that are shown in the include section of both scripts if you do not already 
have them locally.

These were written using Python2.7 

### Manifest tool:
The make manifest tool simply creates a manifest file in csv format consisting of:

<aws path/aws file name>:<file version>

You can view the sample manifest included to get an idea of what its creating. 
(fictional... you will NEED to change the values)

**Running the script:**
python s3_make_manifest.py -m <manifest csv file name> -b <s3 bucket name>

### Versioned Manifest Download Tool ###

The second script uses a manifest in the form of the csv as described above 
to download each file to the local file system. 
* Required parameters: 
	* -r: where on disk should we generate the downloaded file tree
	* -m: The manifest csv file either generated above or by hand
	* -b: The s3 bucket name only (you do not need to include the s3://) 

*Note:*
If you run this program against a directory that already exists or partially does there
could be unintended results. If you specify "/" for example (I have added code to try to 
protect against this, but its your foot to shoot...)

**Running the script:**
python s3_get_versioned_files_by_manifest.py -m <Manifest csv file name> -r <path to destination directory> -b <s3 bucket name>

### Performance ###

I have tested the download speed on the s3_get_versioned_files_by_manifest and found it to be pretty fast.
Each of these is with the default settings of 100 files per thread and 10 threads max concurrently. 

**Example Results:**
* Pulling 100 files 
	* Fresh: 5.7 seconds from scratch. 
	* already exists: 1.4 seconds.
* Pulling 1100 files (11MB) 
	* Fresh: 7.9 seconds
	* already exists: 3.65 seconds

**Large results comparison vs. aws cli s3 sync utility :** Our wp-content folder has approx. 14310 files consisting of 484MB
	* AWS s3 sync
		* fresh (empty) folder: 1 minute and 43 seconds 
		* already exists: 6.4 seconds
	* Versioned download
		* fresh (empty) folder: 2 Minutes 7 seconds
		* fresh with async behavior: 2 minutes even
		* already exists: 48 Seconds
		* already exists with async behavior: 46.87 (suprising not much change)

### Summary ###

Ok, so, the problem we were trying to solve was to be able to download a specific list of versioned files
of an S3 bucket. This has been accomplished by creating a "point in time" manifest from a s3 bucket (which you 
could also hand generate to put specific files / versions in place) and then using our utility to read and download
those files to a new output directory. 

This appears to not only be successful but relatively close to native sync speeds for the clean download. 

After the first time I ran the tool I added the ability to identify files that already exist locally based on the md5 
of the file (against the s3 objects etag, which is its md5 signature). This cut down on unnecessary 
file transfers. 

This provides the basic working example of versioned downloads. Feel free to use/ignore whatever
you can from it.

### Whats next? ###
I may add the ability to:
* synchronize deletes 
* ignore specific regex directories/files  
* Identify faster way to index md5 sums of files so we dont need to recompute them
* Reestablishing new threads and new connections cleans out memory for the thread (in theory) 
but it might make more sense to create a "work" queue and let the threads keep doing work so as not to reestablish a new rpc/auth cycle for each chunk
(which would be around 141 cycles or so for the above working set)




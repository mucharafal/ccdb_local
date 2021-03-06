# Synchronization tool between two CCDB repositories

## Description
A simple tool to import part of the CCDB namespace in a local repository.
The `ch.alice.o2.ccdb.tools.Synchronization` class is included in all CCDB jars, in particular the [local](http://alimonitor.cern.ch/download/local.jar) version.

## Execution
Target use case is retrieving part of the object tree in a local instance.
So make sure you [start](https://docs.google.com/document/d/1_GM6yY7ejVEIRi1y8Ooc9ongrGgZyCiks6Ca0OAEav8/edit?usp=sharing) it and you have access to it.
If the target repository is not running on the local machine or access to it from other machines is needed, the `TOMCAT_ADDRESS` environment variable should be set to `*` to be reachable by the intended clients.

The Synchronization tool acts like a client to both source and target and thus the regular access restrictions apply. The above use case (production -> local synchronization) doesn't need any special permissions.

Running is calling the class with exactly three arguments:
- URL of the source repository (for example `http://ccdb-test.cern.ch:8080`)
- URL of the target repository (i.e. `http://localhost:8080`)
- path to synchronize (i.e. `/qc/ITS`)

A full command example is:
```
java -classpath local.jar ch.alice.o2.ccdb.tools.Synchronization http://ccdb-test.cern.ch:8080 http://localhost:8080 /qc/ITS
```

As result of the execution all new objects found under that path are pushed to the target. So it can be run repeatedly and only the extra content will be imported.

Careful though about how many objects are in each namespace, it might take a while to run. Check by accessing something like [http://ccdb-test.cern.ch/browse/qc/ITS?report=true](http://ccdb-test.cern.ch/browse/qc/ITS?report=true) to see how many objects and what is the total volume to be synchronized.

The tool will also report how many objects are found on each side, and a end of the run statistics. Here is an example run with the above parameters:
```
Synchronizing http://ccdb-test.cern.ch:8080/qc/ITS to http://localhost:8080/qc/ITS
Target found 288590 objects under /qc/ITS
Source reports 259300 objects under /qc/ITS
Using SYNCHRONIZATION_THREADS=8 to perform the synchronization
1/259300: qc/ITS/MO/ItsClusterOnline/Layer2/Stave5/CHIP1/ClusterTopology/8e09f760-1eaf-11eb-9067-808d5c385566 already exists
...
258630/259432: qc/ITS/MO/ITSFHR/Occupancy/Layer1/Stave5/Layer1Stave5HITMAP/7fccce70-308e-11eb-8bac-808d5c295566 is missing, downloading (9 KB) ... uploading ... done
...
Skipped: 258625 files, failed to get: 0 files, synchronized: 807 files (5.764 MB, 1.56 MB/s)
```

For transferring the data an intermediate staging area is used, the tool creating a `./temp/` subdirectory to the current working directory from where the command line is started.
Intermediate files are normally deleted immediately after the upload, so the disk space requirements are only as large as the largest file to transfer. At the end of the execution this directory can/should be removed.

## Concurrency
To speed up the process a thread pool is used to parallelize the actual transfer. The default number of threads is 8. This can be tuned with the `synchronization.threads` configuration key, with either
```
java -Dsynchronization.threads=4 -classpath...
```
or exporting it as an environment variable:
```
export SYNCHRONIZATION_THREADS=4
java -classpath...
```

Careful when changing this value. A too low value might not fully use the available bandwidth while a too high value might cause overloads of the source or the target CCDB repositories.

This repository contains all the scripts use for the Landing Zone part of the Big Data Management Project of BDMA.

Team: Team-BDMA12-A2 (DataImporta)
    - Niccolò Morabito
    - Víctor Diví i Cuesta

VMs used:
    - tentacool.fib.upc.edu : Contains the HDFS manager
    - victreebel.fib.upc.edu : Contains the HBase manager
    - Credentials for both machines: bdm:bdm

Project structure:
    - Settings: Contains variable shared across all project, such as HDFS and HBase server urls
    - Collectors: Contains the different collector scripts that scrape the data and load it to HDFS. We recommend running
                    the scripts in this folder from the virtual machine where HDFS is located, since the file keeping
                    track of the already seen files are local (as future work, we should add them to HDFS).
    - Persistence: Contains the scripts to start the HBase environment (startHBase.sh) and to create and drop tables
    - Loaders: Contain the script that loads the data from HDFS to HBase
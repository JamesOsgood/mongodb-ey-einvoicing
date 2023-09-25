# Import

## Installation

Install

* python3

sudo pip3 install pymongo dnspython pysys

## Preparation

``` 
    cd testcases

    cp unix.properties.tmpl unix.properties
```

Edit `unix.properties` to point to

* Data dir for files
* MongoDB Atlas Connection String

## Atlas Prep

1. Create an Atlas Cluster. 
2. Create an Atlas user
3. Update the connection string in `unix.properties`


## Running the tests
The imports are written as tests using a Python test framework called [Pysys](https://github.com/pysys-test/pysys-test), which is framework designed to assist in creating system level test. Each pysys test is in a directory with test code (```run.py```) and directories for input and output.

To run a test, open a command shell and navigate to the testcases folder. In that folder there are separate folders for the tests. 

1. `pysys run import_data` - This imports all sample xml files


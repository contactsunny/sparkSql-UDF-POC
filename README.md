# SparkSQL UDF POC

This is a simple Java program to illustrate the use of User Defined Functions (UDFs) in Apache SparkSql.

This POC will show you how to register, define, and call a UDF on a dataset. There are examples of how to pass parameters to the UDF as well.

## Inputs

As of now, there's only one input as a command line argument to the program. You need to pass the input file path to the program.

## Input File

The input file should be a ```.csv``` with the columns ```name``` and ```number```. A sample is included in the root of the project as an example. The file content is as follows:

```csv
name,number
name1,1
name2,2
name3,3
name4,4
```

The first row of the CSV file will be ignored as it is assumed to be the header.

## Running The Project

You need to build the project first before running it. You can build it by running the following command from the root of the project directory:

```shell
mvn clean install
```

After this, run the following command to run the project from the same directory:

```shell
java -jar target/SparkSqlUDF-POC-1.0-SNAPSHOT.jar inputFile.csv
```

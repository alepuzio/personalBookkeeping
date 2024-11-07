# Structure of the project

The project is structured following [these indications](https://pyspark-tdd-template.readthedocs.io/en/latest/intro.html)

## Structure of the directories

* scripts: scripts, depending of the platform, to run the project
* configs: configuration files;
* dependencies: third-party libraries
* ddl: DDL script for the database
* jobs: ETL jobs, batch
* tests: internal tests
	* test_data: data of the internal tests
		* input: input data for the internal tests
		* output: output data for the internal tests

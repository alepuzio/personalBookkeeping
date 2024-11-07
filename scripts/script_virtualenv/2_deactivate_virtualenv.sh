#!/usr/bin/bash

# deactivation
python3 -m venv pyspark
echo "Start deactivation virtualenv pyspark"
source pyspark/bin/deactivate
echo "End deactivation virtualenv pyspark"

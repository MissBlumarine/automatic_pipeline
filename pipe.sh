#!/bin/bash
/opt/spark/bin/spark-submit --driver-class-path postgresql-42.4.1.jar --jars postgresql-42.4.1.jar load_dds.py
/opt/spark/bin/spark-submit --driver-class-path postgresql-42.4.1.jar --jars postgresql-42.4.1.jar load_data_mart.py

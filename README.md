# IDML Spark Example

This example code shows how you can use IDML mapping as part of your Spark pipeline.

## Dependencies

* Maven (v3.0.5 or above)
* Apache Spark (v1.5.0)
* DataSift Ptolemy (v1.0.148)
* Gson (v2.3.0)
* IDML-Spark (https://github.com/IDML/idml-spark)

## Build

Run...

``mvn package``

This will create a thin and shaded JAR. You'll need the shaded JAR for deployment to Spark.

## Run

Run spark-submit as follows...

``./bin/spark-submit --class --class org.idml.sparkexample.IDMLExample --master local[<number of cores, e.g. 2>] <path to built shaded JAR> <path to IDML mapping file> <Path to line-delimited JSON input file>``
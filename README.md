# Setting-up-Spark-with-Maven
First Spark tutorial solution with explanation

After cloning the repository you have cd to main my-app directory to execute maven build tool.
Execution below:
"mvn compile assembly:single"

## Download Files From
http://www.wikibench.eu/wiki/2007-09/

## Exact command
**SPARK_LOCAL_IP=localhost** java -cp target/my-app-1.0-jar-with-dependencies.jar TransformRDD <file-name>
**SPARK_LOCAL_IP=localhost** java -cp target/my-app-1.0-jar-with-dependencies.jar Flights <file-name> <city-name>

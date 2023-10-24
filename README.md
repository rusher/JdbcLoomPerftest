# Testing virtual thread performance with MySQL/MariaDB JDBC connectors

Project Loom is now in LTS version java 21.

This is a JMH benchmark in order to benchmark use of standard thread vs virtual threads.

## How to run :

```script
# build fat jar
mvn clean package

# run the bench
java -Duser.country=US -Duser.language=en -jar target/benchmarks.jar
```
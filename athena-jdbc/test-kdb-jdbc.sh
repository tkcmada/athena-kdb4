#!/bin/sh

javac -classpath target/athena-jdbc-1.0.jar TestKdbJdbc.java || exit 1
echo "compiled."
java -classpath .:target/athena-jdbc-1.0.jar TestKdbJdbc || exit 1
echo "done."

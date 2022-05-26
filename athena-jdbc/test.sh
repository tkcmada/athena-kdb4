#!/bin/sh
mvn -o -llr clean test -Dcheckstyle.skip '-Dtest=Kdb*Test'

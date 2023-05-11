#!/bin/bash

JAVA_HOME=$(/usr/libexec/java_home -v1.8)
export JAVA_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
mvn clean source:jar install -Pdist -Dspark.version=3.1.3 -Djava.version=1.8

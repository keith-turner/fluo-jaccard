#!/bin/bash

#This script will copy the fluo-jaccard jar and its dependencies to the Fluo
#application lib dir


if [ "$#" -ne 2 ]; then
  echo "Usage : $0 <FLUO HOME> <JACCARD_HOME>"
  exit 
fi

FLUO_HOME=$1
JACCARD_HOME=$2

JAR=$JACCARD_HOME/target/fluo-jaccard-0.0.1-SNAPSHOT.jar

#build and copy phrasecount jar
(cd $JACCARD_HOME; mvn clean package -DskipTests)

FLUO_APP_LIB=$FLUO_HOME/apps/jaccard/lib/

cp $JAR $FLUO_APP_LIB
(cd $JACCARD_HOME; mvn dependency:copy-dependencies -DoutputDirectory=$FLUO_APP_LIB)


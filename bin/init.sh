#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
JACCARD_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

if [ "$#" -ne 0 ]; then
  echo "Usage : $0"
  exit
fi

#ensure $FLUO_HOME is set
if [ -z "$FLUO_HOME" ]; then
  echo '$FLUO_HOME must be set!'
  exit 1
fi

#Set application name.  $FLUO_APP_NAME is set by fluo-dev and zetten
APP=${FLUO_APP_NAME:-jaccard}

#derived variables
APP_PROPS=$FLUO_HOME/apps/$APP/conf/fluo.properties

if [ ! -f $FLUO_HOME/conf/fluo.properties ]; then
  echo "Fluo is not configured, exiting."
  exit 1
fi

#remove application if it exists
if [ -d $FLUO_HOME/apps/$APP ]; then
  echo "Restarting '$APP' application.  Errors may be printed if it's not running..."
  $FLUO_HOME/bin/fluo stop $APP || true
  rm -rf $FLUO_HOME/apps/$APP
fi

#create new application dir
$FLUO_HOME/bin/fluo new $APP

#copy phrasecount jars to Fluo application lib dir
$JACCARD_HOME/bin/copy-jars.sh $FLUO_HOME $JACCARD_HOME

#Create export table and output Fluo configuration
$FLUO_HOME/bin/fluo exec $APP fj.cmd.CreateExport
#grep -v because of fluo-io/fluo#629
$FLUO_HOME/bin/fluo exec $APP fj.cmd.GenConfig | grep -v "Connecting to" >> $APP_PROPS

$FLUO_HOME/bin/fluo init $APP -f
$FLUO_HOME/bin/fluo exec $APP io.fluo.recipes.accumulo.cmds.OptimizeTable
$FLUO_HOME/bin/fluo start $APP

#!/bin/bash

DRY_RUN=0

CURRENT_TIME=$(date +'%Y%m%d%H%M%S')
PROGRAM_RELATIVE_PATH=$1
PROGRAM_DIR=$(dirname $PROGRAM_RELATIVE_PATH)
PROGRAM_NAME=$(basename $PROGRAM_RELATIVE_PATH)
SCRIPT_FULL_PATH=$(realpath $0)
SCRIPT_DIR=$(dirname $SCRIPT_FULL_PATH)
LOG_DIR="$SCRIPT_DIR/logs/$PROGRAM_NAME/$CURRENT_TIME"
IS_PYTHON=$(echo $PROGRAM_NAME | grep -coP ".py")
if [ ! -e $PROGRAM_RELATIVE_PATH ]; then
    echo "Error: $PROGRAM_RELATIVE_PATH does not exist"
    exit 1
fi
if [ ! -x $PROGRAM_RELATIVE_PATH ]; then
    if [ $IS_PYTHON -eq 0 ]; then
        echo "Error: $PROGRAM_RELATIVE_PATH is not executable"
        exit 1
    fi
fi

LOG_PATH="$LOG_DIR/out"
PID_PATH="$LOG_DIR/pid"

if [ $IS_PYTHON -eq 1 ]; then
    PROGRAM_RELATIVE_PATH="python3 -u $PROGRAM_RELATIVE_PATH"
fi

if [ $DRY_RUN -eq 0 ]; then
    mkdir -p $LOG_DIR
    if [ ! -d $LOG_DIR ]; then
        echo "Error: Could not create log directory $LOG_DIR"
        exit 1
    fi
    touch $LOG_PATH
    if [ ! -f $LOG_PATH ]; then
        echo "Error: Could not create log file $LOG_PATH"
        exit 1
    fi
    touch $PID_PATH
    if [ ! -f $PID_PATH ]; then
        echo "Error: Could not create pid file $PID_PATH"
        exit 1
    fi
    echo "Starting '$PROGRAM_RELATIVE_PATH' and logging into $LOG_PATH"
    nohup $PROGRAM_RELATIVE_PATH > $LOG_PATH 2>&1 &
    echo $! > $PID_PATH
fi

echo $LOG_DIR

#!/bin/bash

REVISION_FILE="revision.txt"

# Check if the file exists and read from it, otherwise initialize to 0
if [ -f "$REVISION_FILE" ]; then
    REVISION=$(cat $REVISION_FILE)
else
    REVISION=0
fi

# Increment the revision number
REVISION=$((REVISION+1))

# Store the new revision number back into the file
echo $REVISION > $REVISION_FILE

# Use the new revision number in your docker commands
docker build --platform linux/amd64 --tag xbocks/telenews:MessageEmbed-r$REVISION -f ./Dockerfile.MessageEmbed .
docker push xbocks/telenews:MessageEmbed-r$REVISION
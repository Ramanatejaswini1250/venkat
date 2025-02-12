#!/bin/bash

# Set variables
HDFS_PATH="/path/to/hdfs/directory"
LOCAL_PATH="/path/to/local/directory"

# Remote locations
REMOTE_USER1="remote_user1"
REMOTE_HOST1="remote_host1"
REMOTE_PATH1="/path/to/remote/directory1"

REMOTE_USER2="remote_user2"
REMOTE_HOST2="remote_host2"
REMOTE_PATH2="/path/to/remote/directory2"

# Find the latest RBCSS file in HDFS
LATEST_FILE=$(hadoop fs -ls "$HDFS_PATH" | grep "RBCSS" | awk '{print $6, $7, $8}' | sort -r | head -n 1 | awk '{print $3}')

if [ -n "$LATEST_FILE" ]; then
  echo "Latest RBCSS file found: $LATEST_FILE. Copying to local..."

  # Copy the latest RBCSS file from HDFS to local
  hadoop fs -copyToLocal "$LATEST_FILE" "$LOCAL_PATH"

  # Check if the copied file contains "RBSCC"
  FILE_NAME=$(basename "$LATEST_FILE")
  if echo "$FILE_NAME" | grep -q "RBSCC"; then
    echo "RBSCC file found. Transferring via SCP to both locations..."

    # Transfer to first remote location
    scp "$LOCAL_PATH/$FILE_NAME" "$REMOTE_USER1@$REMOTE_HOST1:$REMOTE_PATH1"
    if [ $? -eq 0 ]; then
      echo "File transferred successfully to $REMOTE_HOST1."
    else
      echo "Failed to transfer file to $REMOTE_HOST1."
    fi

    # Transfer to second remote location
    scp "$LOCAL_PATH/$FILE_NAME" "$REMOTE_USER2@$REMOTE_HOST2:$REMOTE_PATH2"
    if [ $? -eq 0 ]; then
      echo "File transferred successfully to $REMOTE_HOST2."
    else
      echo "Failed to transfer file to $REMOTE_HOST2."
    fi
  else
    echo "Copied file does not contain 'RBSCC'. No transfer performed."
  fi
else
  echo "No RBCSS files found in HDFS."
fi

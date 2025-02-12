#!/bin/bash

# Set variables
HDFS_PATH="/path/to/hdfs/directory"
LOCAL_PATH="/path/to/local/directory"
REMOTE_USER="remote_user"
REMOTE_HOST="remote_host"
REMOTE_PATH="/path/to/remote/directory"

# Check if RBCSS files exist in HDFS
hadoop fs -ls "$HDFS_PATH" | grep "RBCSS"
if [ $? -eq 0 ]; then
  echo "RBCSS files found in HDFS. Copying to local..."

  # Copy RBCSS files from HDFS to local
  hadoop fs -copyToLocal "$HDFS_PATH/*RBCSS*" "$LOCAL_PATH"

  # Check if files with RBSCC exist in local
  ls "$LOCAL_PATH" | grep "RBSCC"
  if [ $? -eq 0 ]; then
    echo "RBSCC files found in local. Transferring via SCP..."

    # Transfer RBSCC files using SCP
    scp "$LOCAL_PATH/*RBSCC*" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH"
    if [ $? -eq 0 ]; then
      echo "Files transferred successfully via SCP."
    else
      echo "Failed to transfer files via SCP."
    fi
  else
    echo "No RBSCC files found in local."
  fi
else
  echo "No RBCSS files found in HDFS."
fi

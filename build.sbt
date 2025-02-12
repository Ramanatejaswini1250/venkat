#!/bin/bash

# HDFS directory where files are stored
HDFS_DIR="/tmp/ramp"

# Local directory to copy files to
LOCAL_DIR="/tmp/ramp/hdfs_copy"

# Ensure the local directory exists
mkdir -p "$LOCAL_DIR"

# Find the latest folder matching the pattern *_RBSCSS_Wed in HDFS
LATEST_FOLDER=$(hadoop fs -ls "$HDFS_DIR" | grep '_RBSCSS_Wed' | awk '{print $8}' | sort | tail -n 1)

# Check if a folder was found
if [ -z "$LATEST_FOLDER" ]; then
  echo "No folder found with pattern *_RBSCSS_Wed in $HDFS_DIR"
  exit 1
fi

# Copy the folder to the local directory
hadoop fs -copyToLocal "$LATEST_FOLDER" "$LOCAL_DIR"

if [ $? -eq 0 ]; then
  echo "Folder $LATEST_FOLDER copied successfully to $LOCAL_DIR"
else
  echo "Error copying folder $LATEST_FOLDER to $LOCAL_DIR"
  exit 1
fi

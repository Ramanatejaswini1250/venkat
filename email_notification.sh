#!/bin/bash

# Exit immediately if any command exits with a non-zero status
set -e

# Check if at least one argument (alert code) is passed
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <alertCode> [<message>]"
  exit 1
fi

# Get the arguments
ALERT_CODE=$1
MESSAGE=${2:-"No additional details provided."}  # Default message if not provided

# Email details
EMAIL_SUBJECT="Alert Notification: ${ALERT_CODE}"
EMAIL_BODY="An alert has been triggered.\n\nAlert Code: ${ALERT_CODE}\nDetails: ${MESSAGE}\n\nPlease take necessary action."
EMAIL_TO="recipient@example.com"  # Replace with actual recipient email address
EMAIL_FROM="noreply@example.com"  # Replace with actual sender email address

# Send the email
echo -e "$EMAIL_BODY" | mail -s "$EMAIL_SUBJECT" -r "$EMAIL_FROM" "$EMAIL_TO"

# Log success
echo "Email notification sent for Alert Code: $ALERT_CODE"

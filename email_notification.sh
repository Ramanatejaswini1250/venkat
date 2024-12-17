#!/bin/bash

# Ensure proper usage of the script
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <alertCode> <message> <emailAddress> <business>"
    exit 1
fi

ALERT_CODE="$1"
MESSAGE="$2"
EMAIL_ADDRESS="$3"
BUSINESS="$4"

# SMTP Configuration (update with actual details)
SMTP_SERVER="smtp.yourmailserver.com"        # Replace with your SMTP server
SMTP_PORT=587                                # Port for TLS
SMTP_USER="your_email@example.com"           # Replace with your SMTP username
SMTP_PASSWORD="$SMTP_PASSWORD_ENV_VAR"       # Use environment variable for security

# Email Information
FROM="your_email@example.com"                # Replace with your sender email address
TO="$EMAIL_ADDRESS"                          # Dynamically use the passed email address
SUBJECT="Alert: $ALERT_CODE - $BUSINESS"     # Include business in the subject

# Send email using msmtp (Make sure msmtp is installed on the system)
echo -e "Subject: $SUBJECT\n\nMessage: $MESSAGE\n\nBusiness: $BUSINESS" | msmtp --from="$FROM" --to="$TO" --host="$SMTP_SERVER" --port="$SMTP_PORT" --user="$SMTP_USER" --passwordeval="echo $SMTP_PASSWORD"

# Check if email was sent successfully
if [ $? -eq 0 ]; then
    echo "Email notification sent to $EMAIL_ADDRESS for business $BUSINESS successfully."
else
    echo "Failed to send email notification to $EMAIL_ADDRESS."
fi

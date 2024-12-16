#!/bin/bash

# Ensure proper usage of the script
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <alertCode> <message>"
    exit 1
fi

ALERT_CODE="$1"
MESSAGE="$2"

# SMTP Configuration (update with actual details)
SMTP_SERVER="smtp.yourmailserver.com"        # Replace with your SMTP server
SMTP_PORT=587                                # Port for TLS
SMTP_USER="your_email@example.com"           # Replace with your SMTP username
SMTP_PASSWORD="your_password"               # Replace with your SMTP password

# Email Information
FROM="your_email@example.com"                # Replace with the sender email address
TO="venkat@cba.com.au"                       # The recipient email (Venkat)
SUBJECT="Alert: $ALERT_CODE"

# Send email using msmtp (Make sure msmtp is installed on the system)
echo -e "Subject: $SUBJECT\n\n$MESSAGE" | msmtp --from="$FROM" --to="$TO" --host="$SMTP_SERVER" --port="$SMTP_PORT" --user="$SMTP_USER" --passwordeval="echo $SMTP_PASSWORD"

# Check if email was sent successfully
if [ $? -eq 0 ]; then
    echo "Email notification sent to venkat@cba.com.au successfully."
else
    echo "Failed to send email notification to venkat@cba.com.au."
fi

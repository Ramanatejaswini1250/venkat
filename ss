#!/bin/bash

# mail_notification.sh

# Load SMTP configuration from properties file
PROPERTIES_FILE="/path/to/smtp_config.properties"

# Function to load property from properties file
get_property() {
  key=$1
  value=$(grep -m 1 "^$key=" "$PROPERTIES_FILE" | cut -d'=' -f2-)
  echo "$value"
}

# Read properties from the configuration file
SMTP_HOST=$(get_property "smtp_host")
SMTP_PORT=$(get_property "smtp_port")
SMTP_USERNAME=$(get_property "smtp_username")
SMTP_PASSWORD=$(get_property "smtp_password")
FROM_EMAIL=$(get_property "from_email")

# Read arguments passed from the Scala application
ALERT_CODE=$1
MESSAGE=$2
TO_EMAIL=$3
CC_EMAIL=$4
BUSINESS=$5

# Create the email subject and body
SUBJECT="Alert: $ALERT_CODE - $BUSINESS"
BODY="Alert Code: $ALERT_CODE\nMessage: $MESSAGE\nBusiness: $BUSINESS\n\nPlease check the issue."

# Function to send email using mailx
send_email() {
  echo -e "$BODY" | mailx -v -s "$SUBJECT" -r "$FROM_EMAIL" -S smtp="$SMTP_HOST:$SMTP_PORT" -S smtp-auth=login -S smtp-auth-user="$SMTP_USERNAME" -S smtp-auth-password="$SMTP_PASSWORD" -S ssl-verify=ignore "$TO_EMAIL"

  # Check if the email was sent successfully
  if [ $? -eq 0 ]; then
    echo "Email sent to $TO_EMAIL and CC: $CC_EMAIL"
  else
    echo "Failed to send email"
  fi
}

# Send the email
send_email

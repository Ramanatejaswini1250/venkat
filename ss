# Function to send email using msmtp
send_email() {
  echo -e "Subject: $SUBJECT\nTo: $TO_EMAIL\nCc: $CC_EMAIL\nContent-Type: text/plain; charset=UTF-8\n\n$BODY" | msmtp "$TO_EMAIL"

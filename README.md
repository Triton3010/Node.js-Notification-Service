# Node.js-Notification-Service
Notification pushing service in Node.js
A Node.js service which pushes notification to users when they reach a required age in weeks. 
The sent data is stored to the AWS server in json format 
Whenever it attempts to send a notification, it first matches the sent data json to see if that messageID is already sent to that user.
AWS configkey, params and other details are removed for security reasons
Now in production state

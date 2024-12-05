#!/bin/bash

# Update package lists and install required dependencies
sudo apt-get update -y
sudo apt-get install -y default-jdk wget unzip awscli

# Verify Java installation
java -version

# Set variables for script
S3_BUCKET="yuval-hagar-best-bucket"
S3_MANAGER_JAR="manager.jar"
LOCAL_APP_DIR="/home/ubuntu/app"

# Create application directory
mkdir -p $LOCAL_APP_DIR
cd $LOCAL_APP_DIR

# Download the Manager JAR file from S3
aws s3 cp s3://$S3_BUCKET/$S3_MANAGER_JAR ./manager.jar

# Ensure the JAR file is executable
chmod +x manager.jar

# Start the Manager node
echo "Starting Manager node..."
java -jar manager.jar

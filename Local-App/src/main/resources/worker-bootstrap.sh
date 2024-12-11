#!/bin/bash

set -e # Exit immediately if a command exits with a non-zero status

# Update package lists
sudo yum update -y

# Check if Java is installed
if ! java -version &>/dev/null; then
    echo "Java is not installed. Installing Java 8..."
    sudo yum install -y java-1.8.0-openjdk
else
    echo "Java is already installed. Skipping installation."
fi

# Verify Java installation
java -version

# Set variables for script
S3_BUCKET="yuval-hagar-best-bucket"
S3_WORKER_JAR="worker.jar"
LOCAL_APP_DIR="/home/ec2-user/app"

# Create application directory
mkdir -p $LOCAL_APP_DIR
cd $LOCAL_APP_DIR

# Download the Worker JAR file from S3
aws s3 cp s3://$S3_BUCKET/$S3_WORKER_JAR ./worker.jar

# Ensure the JAR file is executable
chmod +x worker.jar

# Start the Worker node
echo "Starting Worker node..."
java -jar worker.jar > worker.log 2>&1 &

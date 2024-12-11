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
    sudo yum install -y aws-cli
# Verify Java installation
java -version

# Set variables for script
S3_BUCKET="yuval-hagar-best-bucket"
S3_MANAGER_JAR="manager.jar"
LOCAL_APP_DIR="/home/ec2-user/app"

# Create application directory
mkdir -p $LOCAL_APP_DIR
cd $LOCAL_APP_DIR


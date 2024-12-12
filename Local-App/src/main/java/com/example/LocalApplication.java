package com.example;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.*;


import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.time.Instant;
import java.util.List;
import java.util.Scanner;
public class LocalApplication {

    final static AWS aws = AWS.getInstance();
    private static String inputQueueUrl;
    private static String summaryQueueUrl;
    private static String inFilePath;
    private static String outFilePath;
    private static Message summary = null;


    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)]
        if (args.length < 3) {
            System.out.println("Usage: com.example.LocalApplication <inputFilePath> <outputFilePath> [tasksPerWorker] [-t]");
            return;
        }

        inFilePath = args[0];
        outFilePath = args[1]; 
        String tasksPerWorker = args[2];
        boolean terminate = (args.length == 4 && args[3].equals("-t"));
        
        String timestamp = String.valueOf(Instant.now().toEpochMilli());
        String uniqueId = timestamp + "-" + new File(inFilePath).getName();
        String keyPath = aws.getInputFileS3Name() + uniqueId;
        
        try {
            setup();
            // upload input file to S3
            aws.uploadFileToS3(keyPath, new File(inFilePath)); 

            // send message to inputQueue
            String messageBody = String.format("%s\t%s\t%s", keyPath, tasksPerWorker, terminate);
            String msgId = aws.sendMessage(inputQueueUrl, messageBody); 

            // "subscribe" to correct summary queue
            int summaryNum = (msgId.hashCode() & Integer.MAX_VALUE) % aws.getSummaryLimit() + 1;
            summaryQueueUrl = aws.getQueueUrl("summaryQueue_" + summaryNum); 
            while (summary == null) {
                summary = aws.receiveMessageWithId(summaryQueueUrl, msgId);

            }
            System.out.println("Got the summary file" + summary.body());

            // download summary from s3
            File summaryFile = new File("local-summary.txt");
            aws.downloadFileFromS3(summary.body(), summaryFile);

            // creates html output file
            summaryToHTML(summaryFile);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void setup() {
        try {
            List<Instance> list = aws.getAllInstancesWithLabel(AWS.Label.Manager);
            if (list.isEmpty()) { // if manager is not active
                aws.createBucketIfNotExists(aws.getBucketName());
                inputQueueUrl = aws.createQueue(aws.getInputQueueName());
                for (int i = 1; i <= aws.getSummaryLimit(); i++) {
                    String name = "summaryQueue_" + i;
                    aws.createQueue(name);
                }

                // Dynamically locate resources
                File managerJar = getResourceFile("Manager-1.0-SNAPSHOT.jar");
                File workerJar = getResourceFile("Worker-1.0-SNAPSHOT.jar");

                if (managerJar == null || workerJar == null) {
                    System.err.println("Required resources are missing. Aborting setup.");
                    return;
                }

                // Upload files to S3
                aws.uploadFileToS3("manager.jar", managerJar);
                aws.uploadFileToS3("worker.jar", workerJar);

                // EC2 bootstrap script for Manager
                String managerScript = "#!/bin/bash\n" +
                        "set -e # Exit immediately if a command exits with a non-zero status\n" +
                        "\n" +
                        "# Update package lists\n" +
                        "echo \"Updating package lists...\"\n" +
                        "sudo yum update -y\n" +
                        "\n" +
                        "# Install Java if not already installed\n" +
                        "if ! java -version &>/dev/null; then\n" +
                        "    echo \"Java is not installed. Installing Java...\"\n" +
                        "    sudo yum install -y java-1.8.0-openjdk\n" +
                        "else\n" +
                        "    echo \"Java is already installed. Skipping installation.\"\n" +
                        "fi\n" +
                        "\n" +
                        "# Install com.example.AWS CLI if not already installed\n" +
                        "if ! aws --version &>/dev/null; then\n" +
                        "    echo \"com.example.AWS CLI is not installed. Installing com.example.AWS CLI...\"\n" +
                        "    sudo yum install -y aws-cli\n" +
                        "else\n" +
                        "    echo \"com.example.AWS CLI is already installed. Skipping installation.\"\n" +
                        "fi\n" +
                        "\n" +
                        "# Verify Java installation\n" +
                        "echo \"Verifying Java installation...\"\n" +
                        "java -version\n" +
                        "\n" +
                        "# Define variables\n" +
                        "S3_BUCKET=\"yuval-hagar-best-bucket\"\n" +
                        "S3_MANAGER_JAR=\"manager.jar\"\n" +
                        "LOCAL_APP_DIR=\"/home/ec2-user/app\"\n" +
                        "\n" +
                        "# Create application directory and navigate to it\n" +
                        "echo \"Creating application directory at $LOCAL_APP_DIR...\"\n" +
                        "mkdir -p $LOCAL_APP_DIR\n" +
                        "cd $LOCAL_APP_DIR\n" +
                        "\n" +
                        "# Download the manager.jar file from S3\n" +
                        "echo \"Downloading manager.jar from S3...\"\n" +
                        "aws s3 cp s3://$S3_BUCKET/$S3_MANAGER_JAR ./manager.jar\n" +
                        "\n" +
                        "# Make the JAR file executable\n" +
                        "echo \"Making manager.jar executable...\"\n" +
                        "chmod +x manager.jar\n" +
                        "\n" +
                        "# Run the manager.jar file in the background and output to console\n" +
                        "echo \"Starting manager.jar...\"\n" +
                        "java -jar manager.jar &\n" +
                        "\n";

                aws.createEC2WithLimit(managerScript, "Manager", 1); // Create manager EC2
            }
        } catch (Exception e) {
            System.err.println("Error during setup: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static File getResourceFile(String resourceName) {
        URL resource = LocalApplication.class.getClassLoader().getResource(resourceName);
        if (resource == null) {
            System.err.println("Resource not found: " + resourceName);
            return null;
        }
        return new File(resource.getPath());
    }



    private static void summaryToHTML(File summaryFile) {
        File htmlOutputFile = new File(outFilePath);
        try (PrintWriter writer = new PrintWriter(htmlOutputFile)) {
            // Read the summary file line by line
            try (Scanner scanner = new Scanner(summaryFile)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    String[] parts = line.split("\t");

                    if (parts.length < 3) {
                        writer.println("Invalid summary line: " + line);
                        continue;
                    }

                    String operation = parts[0];
                    String inputFile = parts[1];
                    String result = parts[2];

                    // Generate HTML line
                    String htmlLine;
                    if (result.startsWith("http")) {
                        // If the result is a valid URL, create links for input and output files
                        htmlLine = String.format("<p>%s: <a href='%s'>%s</a> <a href='%s'>%s</a></p>",
                                operation, inputFile, inputFile, result, result);
                    } else {
                        // If an exception occurred or the file is not available, show the result as a description
                        htmlLine = String.format("<p>%s: <a href='%s'>%s</a> %s</p>", operation, inputFile, inputFile, result);
                    }
                    writer.println(htmlLine);
                }
            }
            System.out.println("HTML output file created at: " + htmlOutputFile.getAbsolutePath());
        } catch (Exception e) {
            System.err.println("Error while creating HTML output file: " + e.getMessage());
        }
    }
}

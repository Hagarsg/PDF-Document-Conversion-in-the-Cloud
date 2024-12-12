package com.example;//import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
//import com.amazonaws.services.sqs.model.AmazonSQSException;
//import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
//import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

//import API.com.example.AWS;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Manager {
    final static AWS aws = AWS.getInstance();
    private static final int MAX_WORKERS = 8;
    private static final AtomicBoolean terminate = new AtomicBoolean(false);
    private static String inputQueueUrl;
    private static String workersQueueUrl;
    private static String responsesQueueUrl;
    private static ConcurrentHashMap<String, Integer> localAppMap;
    private static ExecutorService threadPool;


    public static void main(String[] args) {
        inputQueueUrl = aws.getQueueUrl(aws.getInputQueueName());
        workersQueueUrl = aws.createQueue(aws.getWorkerQueueName());
        responsesQueueUrl = aws.createQueue(aws.getResponsesQueueName());
        localAppMap = new ConcurrentHashMap<>();

        threadPool = Executors.newFixedThreadPool(10);

        // InputReader thread
        Thread inputReader = new Thread(() -> {
            while (!terminate.get()) {
                try {
                    List<Message> inputList = aws.receiveMessages(inputQueueUrl);
                    for (Message input : inputList) {
                        System.out.println("Submitting task for input file: " + input.body());
                        threadPool.submit(new ProcessInputTask(input));
                        if (terminate.get()) {
                            System.out.println("Terminate flag set. Stopping input processing.");
                            break; // Stop taking new messages
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Error in InputReader thread.");
                }
            }
            System.out.println("InputReader stopped accepting new messages.");
        });

        // ResponseReader thread
        Thread responseReader = new Thread(() -> {
            while (!terminate.get()) {
                try {
                    List<Message> responseList = aws.receiveMessages(responsesQueueUrl);
                    if (!responseList.isEmpty()) {
                        System.out.println("Processing response messages.");
                        threadPool.submit(new ProcessResponseTask(responseList));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Error in ResponseReader thread.");
                }
            }
            System.out.println("ResponseReader stopped accepting new messages.");
        });

        // Start both threads
        inputReader.start();
        responseReader.start();

        // Wait for termination
        try {
            inputReader.join();
            responseReader.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Shutdown thread pool
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Forcing thread pool shutdown...");
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            threadPool.shutdownNow();
        }

        terminate();
    }



    public static void processInputFile(Message inputFile) throws Exception {
            String inputFileId = inputFile.messageId();
            String[] messageParts = inputFile.body().split("\t");

            if (messageParts.length < 3) {
                throw new Exception("Invalid message format");
            }
            String keyPath = messageParts[0];
            int tasksPerWorker = Integer.valueOf(messageParts[1]);
            terminate.set(Boolean.parseBoolean(messageParts[2])); // Parse the terminate flag

        // Explicit EC2 path for the input file
            String ec2BaseDir = "/tmp/input-files"; // Change to your desired EC2 directory
            String filePath = ec2BaseDir + File.separator + inputFileId;
            File file = new File(filePath);

            try {
                // Ensure parent directory exists
                File parentDir = file.getParentFile();
                if (!parentDir.exists()) {
                    if (parentDir.mkdirs()) {
                        System.out.println("Created parent directory: " + parentDir.getAbsolutePath());
                    } else {
                        throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
                    }
                }

                // Attempt to create the file
                if (file.createNewFile()) {
                    System.out.println("File created: " + file.getAbsolutePath());
                } else {
                    System.out.println("File already exists: " + file.getAbsolutePath());
                }
            } catch (IOException e) {
                System.out.println("An error occurred while creating the file.");
                e.printStackTrace();
                throw e; // Re-throw exception to handle it in the caller if needed
            }

            aws.downloadFileFromS3(keyPath, file); //download file from s3 to ec2

            List<String> urlsAndOperations = parseInputFile(filePath, inputFileId); // read URLs and operations from the input file

            localAppMap.put(inputFileId, urlsAndOperations.size());
            manageWorkers(urlsAndOperations.size(), tasksPerWorker); // create correct amount of workers

            List<String> sentMessageIds = aws.sendMessagesBatch(workersQueueUrl, urlsAndOperations);
        }


    private static List<String> parseInputFile(String filePath, String inputFileId) {
        List<String> urlsAndOperations = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Trim the line and split into words
                String trimmedLine = line.trim();
                String[] parts = trimmedLine.split("\\s+"); // Split by whitespace

                // Check if the line contains exactly two words
                if (parts.length != 2) {
                    System.out.println("Skipping invalid line: " + trimmedLine);
                    continue; // Skip invalid lines
                }

                // Add valid lines to the list
                urlsAndOperations.add(trimmedLine + "\t" + inputFileId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return urlsAndOperations;
    }


    private static void manageWorkers(int messageCount, int tasksPerWorker) throws Exception {
        int requiredWorkers = Math.min((messageCount + tasksPerWorker - 1) / tasksPerWorker, MAX_WORKERS);

        List<Instance> runningWorkers = null;  // Get the count of currently running workers
        runningWorkers = aws.getAllInstancesWithLabel(AWS.Label.Worker);
        int currentWorkerCount = runningWorkers.size();

        if (currentWorkerCount < requiredWorkers) {
            synchronized (Manager.class) {
                int workersToStart = requiredWorkers - currentWorkerCount;
                System.out.printf("Starting %d new workers...\n", workersToStart);
                String script = "#!/bin/bash\n" +
                        "set -e # Exit immediately if a command exits with a non-zero status\n" +
                        "\n" +
                        "# Update package lists\n" +
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
                        "java -version\n" +
                        "\n" +
                        "# Define variables\n" +
                        "S3_BUCKET=\"yuval-hagar-best-bucket\"\n" +
                        "S3_WORKER_JAR=\"worker.jar\"\n" +
                        "LOCAL_APP_DIR=\"/home/ec2-user/app\"\n" +
                        "\n" +
                        "# Create application directory and navigate to it\n" +
                        "mkdir -p $LOCAL_APP_DIR\n" +
                        "cd $LOCAL_APP_DIR\n" +
                        "\n" +
                        "# Download the worker.jar file from S3\n" +
                        "echo \"Downloading worker.jar from S3...\"\n" +
                        "aws s3 cp s3://$S3_BUCKET/$S3_WORKER_JAR ./worker.jar\n" +
                        "\n" +
                        "# Make the JAR file executable\n" +
                        "chmod +x worker.jar\n" +
                        "\n" +
                        "# Run the worker.jar file in the background and redirect output to a log file\n" +
                        "echo \"Starting worker.jar...\"\n"+
                        "java -jar worker.jar &\n" +

                        "# Capture the PID of the background process\n" +
                        "WORKER_PID=$!\n" +
                        "\n" +
                        "# Wait for a moment to ensure the manager is running\n" +
                        "sleep 2\n" +
                        "\n" +
                        "# Check if the manager process is running\n" +
                        "if ps aux | grep \"[j]ava -jar manager.jar\" > /dev/null; then\n" +
                        "    echo \"manager.jar is running (PID: $MANAGER_PID)\"\n" +
                        "else\n" +
                        "    echo \"Failed to start manager.jar\"\n" +
                        "    exit 1\n" +
                        "fi\n" +
                        "\n" +
                        "# Optionally, display the Java process status\n" +
                        "ps aux | grep java\n" +
                        "\n" +
                        "# End of script\n" +
                        "echo \"Script execution completed.\"\n";


                aws.createEC2WithLimit(script, "Worker", workersToStart);
            }

        }
    }
    private static void processResponsesMessage(List<Message> responseList) {
        for (Message response : responseList) {
            try {

                String messageBody = response.body();
                System.out.println("Received response: " + messageBody);

                // Split the response message by tab separator (\t)
                String[] parts = messageBody.split("\t");

                // Ensure that the response message has the correct number of parts
                if (parts.length == 4) {
                    String operation = parts[0];
                    String pdfUrl = parts[1];
                    String s3ResultsPath = parts[2];
                    String fileId = parts[3];

                    System.out.println("Processing response for fileId: " + fileId);
                    System.out.println("Operation: " + operation + ", PDF URL: " + pdfUrl + ", S3 Results Path: " + s3ResultsPath);

                    // Define the local file for this application
                    String localFilePath = System.getProperty("user.dir") + File.separator + "responses" + File.separator + fileId + ".txt";
                    File localFile = new File(localFilePath);
                    System.out.println("Local file path: " + localFilePath);

                    // Ensure the directory exists
                    File parentDir = localFile.getParentFile();
                    if (!parentDir.exists()) {
                        System.out.println("Creating directory: " + parentDir.getAbsolutePath());
                        if (!parentDir.mkdirs()) {
                            throw new RuntimeException("Failed to create directory for local files: " + parentDir.getAbsolutePath());
                        }
                    }

                    // Append the response data to the file
                    String responseData = String.format("%s\t%s\t%s", operation, pdfUrl, s3ResultsPath);
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(localFile, true))) {
                        writer.write(responseData);
                        writer.newLine();
                        System.out.println("Appended response data to file: " + localFilePath);
                    } catch (IOException e) {
                        System.err.println("Error writing to local file: " + localFilePath);
                        e.printStackTrace();
                    }

                    if (localAppMap.containsKey(fileId)) {
                        System.out.println("Updating localAppMap for fileId: " + fileId);
                        localAppMap.compute(fileId, (key, value) -> (value == null || value <= 0) ? 0 : value - 1);
                        System.out.println("Updated value for fileId " + fileId + ": " + localAppMap.get(fileId));

                        if (localAppMap.remove(fileId, 0)) { // Removes key only if value is 0. Otherwise returns false.
                            System.out.println("Generating summary file for fileId: " + fileId);
                            String summaryFileKey = generateSummaryFile(fileId);
                            System.out.println("Summary file created and uploaded: " + summaryFileKey);
                            int summaryNum = (fileId.hashCode() & Integer.MAX_VALUE) % aws.getSummaryLimit() + 1;
                            String summaryQueueUrl = aws.getQueueUrl("summaryQueue_" + summaryNum);
                            aws.sendMessageWithId(summaryQueueUrl, summaryFileKey, fileId);
                            System.out.println("Summary file sent to: " + summaryQueueUrl + " with fileId: " + fileId);
                        }
                    }


                } else {
                    System.err.println("Invalid message format: " + messageBody);
                }
            } catch (Exception e) {
                System.err.println("Error processing response message: " + response.body());
                e.printStackTrace();

            } finally {
                // Always attempt to delete the message
                try {
                    aws.deleteMessage(responsesQueueUrl, response);
                } catch (Exception e) {
                    System.err.println("Failed to delete response message: " + response.body());
                    e.printStackTrace();
                }
            }
        }
    }
    private static File createTempFile(String content) {
        File tempFile = null;
        try {
            tempFile = File.createTempFile("response", ".txt"); // Create a temporary file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) { // Write the content to the temporary file
                writer.write(content);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create temporary file for upload");
        }
        return tempFile;
    }


    private static String generateSummaryFile(String inputFileId) {
        String localFilePath = System.getProperty("user.dir") + File.separator + "responses" + File.separator + inputFileId + ".txt";
        File localFile = new File(localFilePath);

        if (!localFile.exists()) {
            throw new RuntimeException("Local response file not found: " + localFilePath);
        }

        String summaryFileKey = aws.getSummariesS3Name() + inputFileId + ".txt";

        // Upload the local file directly to S3 as the summary file
        try {
            aws.uploadFileToS3(summaryFileKey, localFile);
            System.out.println("Uploaded summary file to S3: " + summaryFileKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload summary file to S3", e);
        }

        // Delete the local file after uploading
        if (!localFile.delete()) {
            System.err.println("Failed to delete local file: " + localFilePath);
        }

        return summaryFileKey;
    }


    private static String readFileContent(File file) {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append(System.lineSeparator());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content.toString();
    }

    public static void terminate() {
        System.out.println("Termination initiated...");

        // Wait for all jobs to complete
        while (!localAppMap.isEmpty()) {
            try {
                System.out.println("Waiting for all jobs to complete...");
                Thread.sleep(5000); // Check every 5 seconds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("All jobs completed. Generating pending summaries...");
        // generateAllPendingSummaries();

        System.out.println("Shutting down thread pool...");
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Forcing thread pool shutdown...");
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            threadPool.shutdownNow();
        }

        terminateAllWorkers();

        System.out.println("Shutting down the manager instance...");
        List<String> managerIds = aws.getAllInstanceIdsWithLabel(AWS.Label.Manager);
        if (!managerIds.isEmpty()) {
            aws.terminateInstance(managerIds.get(0));
        } else {
            System.err.println("No manager instance found to terminate.");
        }

        System.out.println("Termination process completed.");
    }


//
//    private static void generateAllPendingSummaries() {
//        System.out.println("Generating pending summaries...");
//        for (String inputFileId : localAppMap.keySet()) {
//            if (localAppMap.get(inputFileId) == 0) {
//                generateSummaryFile(inputFileId); // Generate the summary for completed jobs
//            }
//        }
//    }

    private static void terminateAllWorkers() {
        System.out.println("Terminating all worker instances...");
        List<String> workerIds = null;
        workerIds = aws.getAllInstanceIdsWithLabel(AWS.Label.Worker);
        for (String id : workerIds) {
            aws.terminateInstance(id);
            System.out.println("Terminated worker instance: " + id);
        }
    }

    public static boolean isTerminateMessage(String messageBody) {
        if (messageBody.equals("terminate")) {
            terminate.set(true);
            System.out.println("com.example.Manager got terminate message");
            return true;
        }
        return false;
    }


    public static class ProcessInputTask implements Runnable {
        private final Message inputMessage;

        public ProcessInputTask(Message inputMessage) {
            this.inputMessage = inputMessage;
        }

        @Override
        public void run() {
            try {
                Manager.processInputFile(inputMessage);
                aws.deleteMessage(Manager.inputQueueUrl, inputMessage); // Delete processed message
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error processing input file: " + inputMessage.body());
            }
        }
    }

    public static class ProcessResponseTask implements Runnable {
        private final List<Message> responseMessages;

        public ProcessResponseTask(List<Message> responseMessages) {
            this.responseMessages = responseMessages;
        }

        @Override
        public void run() {
            try {
                Manager.processResponsesMessage(responseMessages);
                aws.deleteMessages(responsesQueueUrl, responseMessages);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error processing response messages.");
            }
        }
    }


}


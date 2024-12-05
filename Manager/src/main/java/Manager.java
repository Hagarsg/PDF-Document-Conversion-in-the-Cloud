//import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
//import com.amazonaws.services.sqs.model.AmazonSQSException;
//import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
//import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

//import API.AWS;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Manager {
    final static AWS aws = AWS.getInstance();
    private static final int MAX_WORKERS = 7;
    private static volatile boolean terminate = false;
    private static String inputQueueUrl;
    private static String workersQueueUrl;
    private static String responsesQueueUrl;
    private static HashMap<String, Integer> localAppMap;

public static void main(String[] args) throws Exception {
    inputQueueUrl = aws.getQueueUrl(aws.getInputQueueName());
    workersQueueUrl = aws.createQueue(aws.getWorkerQueueName());
    responsesQueueUrl = aws.createQueue(aws.getResponsesQueueName());
    localAppMap = new HashMap<>();


    while (!terminate){
        //processing input files
        List<Message> inputList = aws.receiveMessages(inputQueueUrl);
        for (Message input : inputList) {
            //processing input files
            processInputFile(input);
            aws.deleteMessage(inputQueueUrl, input); // Correct method for receipt handle
        }
        //processing workers output files
        processResponsesMessage();
    }   
}

public static void processInputFile(Message inputFile) throws Exception {
    if(isTeminateMessage(inputFile.body())){
        terminate();
    } 
    else {
        String inputFileId = inputFile.messageId();
        String[] messageParts = inputFile.body().split("\t");
        String keyPath = messageParts[0];
        int tasksPerWorker = Integer.valueOf(messageParts[1]);
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

        // add debug-- show content of donwloaded file

        List<String> urlsAndOperations = parseInputFile(filePath, inputFileId); // read URLs and operations from the input file

        // add debug-- see that parsing is done right

        localAppMap.put(inputFileId, urlsAndOperations.size());
        manageWorkers(urlsAndOperations.size(), tasksPerWorker); // create correct amount of workers 

        // send url and ops to worker queue
        for (String msg : urlsAndOperations) {
            System.out.println("Sending message to workers Queue: " + msg);
            aws.sendMessage(workersQueueUrl, msg);
        }
        // aws.sendMessageBatches(workersQueueUrl, urlsAndOperations);
    }
}

private static List<String> parseInputFile(String filePath, String inputFileId) {
    List<String> urlsAndOperations = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
        String line;
        while ((line = reader.readLine()) != null) {
            urlsAndOperations.add(line.trim()+ "\t" + inputFileId);
        }
    } catch (IOException e) {
        e.printStackTrace();
    }
    return urlsAndOperations;
}


private static void manageWorkers(int messageCount, int tasksPerWorker) throws Exception {
    int requiredWorkers = Math.min((messageCount + tasksPerWorker - 1) / tasksPerWorker, MAX_WORKERS);

    List<Instance> runningWorkers = null;  // Get the count of currently running workers
    try {
        runningWorkers = aws.getAllInstancesWithLabel(AWS.Label.Worker);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
    int currentWorkerCount = runningWorkers.size();

    if (currentWorkerCount < requiredWorkers) {
        int workersToStart = requiredWorkers - currentWorkerCount;
        System.out.printf("Starting %d new workers...\n", workersToStart);
        String filePath = Manager.class.getClassLoader()
                .getResource("worker-bootstrap.sh")
                .getPath();
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("File does not exist: " + filePath);
            return;
        }
        aws.uploadFileToS3(aws.getScriptPath(AWS.Label.Worker), file);
        String script =  """
                    #!/bin/bash
                    aws s3 cp s3://yuval-hagar-best-bucket/worker-script/worker-bootstrap.sh /tmp/worker-bootstrap.sh
                    chmod +x /tmp/worker-bootstrap.sh
                    /tmp/worker-bootstrap.sh
                    """;
        aws.createEC2WithLimit(script, "Worker", workersToStart);
   }
}

private static void processResponsesMessage(){
    List<Message> responseList = aws.receiveMessages(responsesQueueUrl);
    for(Message response : responseList) {
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

            // Generate a unique key for the individual response
            String responseFileKey = aws.getResponsesS3Name() + fileId + "/" + response.messageId() + ".txt";
            String responseData = String.format("%s\t%s\t%s", operation, pdfUrl, s3ResultsPath);
            File tempFile = createTempFile(responseData);
            // Upload the response to S3
            try {
                aws.uploadFileToS3(responseFileKey, tempFile);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            tempFile.delete();
            
            int cur = localAppMap.get(fileId) - 1;
            if (cur == 0) {
                String summaryFile = generateSummaryFile(fileId);
                System.out.println("Summary file created: " + summaryFile);
                int summaryNum = (fileId.hashCode() & Integer.MAX_VALUE) % aws.getSummaryLimit() + 1;
                String summaryQueueUrl = aws.getQueueUrl("summaryQueue_" + summaryNum); 
                aws.sendMessageWithId(summaryQueueUrl, summaryFile, fileId);
                System.out.println("Summary file sent to: summaryQueue_" + summaryNum + " with  fileId: " + fileId);
            }
            localAppMap.put(fileId,cur);
        } 

        else {
            System.err.println("Invalid message format: " + messageBody);
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
    String responseDirKey = aws.getResponsesS3Name() + inputFileId + "/";
    String summaryFileKey = aws.getSummariesS3Name() + inputFileId + ".txt";

    // List all response files in S3
    List<S3Object> responseFiles = aws.listFilesInS3(responseDirKey);

    // Combine content into a summary file
    StringBuilder summaryContent = new StringBuilder();
    for (S3Object responseFile : responseFiles) {

        // Download the response file content from S3 to a local file
        String filePath = System.getProperty("user.dir") + File.separator + "output-files" + File.separator + inputFileId; //locally on EC2
        File localFile = new File(filePath);
        try {
            // Attempt to create the file
            if (localFile.createNewFile()) {
                System.out.println("File created: " + localFile.getAbsolutePath());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            // Handle potential IO exceptions
            System.out.println("An error occurred while creating the file.");
            e.printStackTrace();
        }
        aws.downloadFileFromS3(responseFile.key(),localFile);
        String fileContent = readFileContent(localFile);
        summaryContent.append(fileContent).append(System.lineSeparator());
        localFile.delete();
    }
    // Create a new file to store the summary
    File summaryFile = new File(System.getProperty("user.dir") + File.separator + "output-files" + File.separator + inputFileId + "-summary.txt");
    try {
        if (!summaryFile.exists()) {
            summaryFile.createNewFile();
        }

        // Write the summary content to the summary file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFile))) {
            writer.write(summaryContent.toString());
        }
    } catch (IOException e) {
        System.out.println("An error occurred while creating the summary file.");
        e.printStackTrace();
    }

    // Upload the summary file to S3
    try {
        return aws.uploadFileToS3(summaryFileKey, summaryFile);
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
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
    while (!(localAppMap.values().stream().allMatch(count -> count == 0))) { //not all job completed
        try {
            Thread.sleep(5000); // Poll every 5 seconds
            System.out.println("Waiting for all jobs to complete...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    generateAllPendingSummaries(); // Create response messages for any completed jobs, if needed
    terminateAllWorkers();
    List<String> managerIds = null;
    try {
        managerIds = aws.getAllInstanceIdsWithLabel(AWS.Label.Manager);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
    aws.terminateInstance(managerIds.get(0)); //terminate the Manager
}

private static void generateAllPendingSummaries() {
    System.out.println("Generating pending summaries...");
    for (String inputFileId : localAppMap.keySet()) {
        if (localAppMap.get(inputFileId) == 0) {
            generateSummaryFile(inputFileId); // Generate the summary for completed jobs
        }
    }
}

private static void terminateAllWorkers() {
    System.out.println("Terminating all worker instances...");
    List<String> workerIds = null;
    try {
        workerIds = aws.getAllInstanceIdsWithLabel(AWS.Label.Worker);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
    for (String id : workerIds) {
        aws.terminateInstance(id);
        System.out.println("Terminated worker instance: " + id);
    }
}

public static boolean isTeminateMessage(String messageBody){
    if (messageBody.equals("terminate")){
        terminate = true;
        System.out.println("Manager got terminate message");
        return true;
    }
    return false;
}

}




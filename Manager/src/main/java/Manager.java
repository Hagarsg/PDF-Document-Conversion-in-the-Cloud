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
    private static final int MAX_WORKERS = 19;
    private static volatile boolean terminate = false;
    private static String inputQueueUrl;
    private static String workersQueueUrl = aws.createQueue("workersQueue");
    private static String responseQueueUrl = aws.createQueue("responseQueue");
    private static HashMap<String, Integer> localAppMap = new HashMap<>();

public static void main(String[] args) {  
    while (!terminate){

        //processing input files
        inputQueueUrl = aws.getQueueUrl("inputQueue");
        List<Message> inputList = aws.receiveMessages(inputQueueUrl);
        for (Message input : inputList) {
            //processing input files
            processInputFile(input);
            aws.deleteMessage(inputQueueUrl, input); // Correct method for receipt handle
        }
        //processing workers output files
        processResponseMessage();
        
    }   
}

public static void processInputFile(Message inputFile){
    if(isTeminateMessage(inputFile.body())){
        terminate();
    } 
    else {
        String inputFileId = inputFile.messageId();
        String[] messageParts = inputFile.body().split("\t");
        String keyPath = messageParts[0];
        int tasksPerWorker = Integer.valueOf(messageParts[1]);
        String filePath = System.getProperty("user.dir") + File.separator + "input-files" + File.separator + inputFileId; //locally on EC2
        File file = new File(filePath);
        try {
            // Attempt to create the file
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getAbsolutePath());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            // Handle potential IO exceptions
            System.out.println("An error occurred while creating the file.");
            e.printStackTrace();
        }
        aws.downloadFileFromS3(keyPath, file);//download file from s3 to ec2
        List<String> urlsAndOperations = parseInputFile(filePath, inputFileId); // read URLs and operations from the input file
        localAppMap.put(inputFileId, urlsAndOperations.size());
        manageWorkers(urlsAndOperations.size(), tasksPerWorker); // create correct amount of workers 

        // send url and ops to worker queue
        for (String msg : urlsAndOperations) {
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


private static void manageWorkers(int messageCount, int tasksPerWorker) {
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
        // String script = "#!/bin/bash\n" +
        // "exec > /var/log/user-data.log 2>&1\n" +
        // "java -jar /home/ec2-user/manager.jar\n"; //change it to worker

//        String script = "#!/bin/bash\n" +
//        "echo Manager jar running\n" +
//        "echo s3://" + aws.getBucketName() + "/" + aws.managerJarKey + "\n" +
//        "mkdir ManagerFiles\n" +
//        "aws s3 cp s3://" + aws.getBucketName() + "/" + aws.managerJarKey + " ./ManagerFiles/" + aws.managerJarName + "\n" +
//        "echo manager copy the jar from s3\n" +
//        "java -jar /ManagerFiles/" + aws.managerJarName + "\n";
//        aws.createEC2(script, "Worker", workersToStart);
   }
}

private static void processResponseMessage(){
    List<Message> responseList = aws.receiveMessages(responseQueueUrl);
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
            String responseFileKey = "responses/" + fileId + "/" + response.messageId() + ".txt";
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
    String responseDirKey = "responses/" + inputFileId + "/";
    String summaryFileKey = "summaries/" + inputFileId + ".txt";

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




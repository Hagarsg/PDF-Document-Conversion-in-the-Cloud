////import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
////import com.amazonaws.services.sqs.model.AmazonSQSException;
////import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
////import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
//
////import API.AWS;
//import software.amazon.awssdk.services.ec2.model.Instance;
//import software.amazon.awssdk.services.s3.model.S3Object;
//import software.amazon.awssdk.services.sqs.model.Message;
//
//import java.io.*;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.logging.*;
//
//
//public class ManagerOld {
//    final static AWS aws = AWS.getInstance();
//    private static final int MAX_WORKERS = 8;
//    private static volatile boolean terminate = false;
//    private static String inputQueueUrl;
//    private static String workersQueueUrl;
//    private static String responsesQueueUrl;
//    private static HashMap<String, Integer> localAppMap;
//    private static final Logger logger = Logger.getLogger(ManagerOld.class.getName());
//
////    static {
////        try {
////            LogManager.getLogManager().reset();
////            ConsoleHandler ch = new ConsoleHandler();
////            ch.setLevel(Level.INFO);
////            logger.addHandler(ch);
////
////            FileHandler fh = new FileHandler("manager.log", true);
////            fh.setFormatter(new SimpleFormatter());
////            fh.setLevel(Level.INFO);
////            logger.addHandler(fh);
////
////            logger.setLevel(Level.INFO);
////        } catch (IOException e) {
////            System.err.println("Could not set up logger: " + e.getMessage());
////            e.printStackTrace();
////        }
////    }
//public static void main(String[] args) throws Exception {
//    inputQueueUrl = aws.getQueueUrl(aws.getInputQueueName());
//    workersQueueUrl = aws.createQueue(aws.getWorkerQueueName());
//    responsesQueueUrl = aws.createQueue(aws.getResponsesQueueName());
//    localAppMap = new HashMap<>();
//
//
//    while (!terminate){
//        //processing input files
//        List<Message> inputList = aws.receiveMessages(inputQueueUrl);
//        for (Message input : inputList) {
//            //processing input files
//            System.out.println("Starting to process input file " + input.body());
//            processInputFile(input);
//            System.out.println("Finished processing input file " + input.body());
//            aws.deleteMessage(inputQueueUrl, input); // Correct method for receipt handle
//        }
//        System.out.println("Outer for loop of manager");
//        //processing workers output files
//        List<Message> responseList = aws.receiveMessages(responsesQueueUrl);
//        processResponsesMessage(responseList);
//    }
//}
//
//public static void processInputFile(Message inputFile) throws Exception {
//    if(isTeminateMessage(inputFile.body())){
//        terminate();
//    }
//    else {
//        String inputFileId = inputFile.messageId();
//        String[] messageParts = inputFile.body().split("\t");
//        String keyPath = messageParts[0];
//        int tasksPerWorker = Integer.valueOf(messageParts[1]);
//        // Explicit EC2 path for the input file
//        String ec2BaseDir = "/tmp/input-files"; // Change to your desired EC2 directory
//        String filePath = ec2BaseDir + File.separator + inputFileId;
//        File file = new File(filePath);
//
//        try {
//            // Ensure parent directory exists
//            File parentDir = file.getParentFile();
//            if (!parentDir.exists()) {
//                if (parentDir.mkdirs()) {
//                    System.out.println("Created parent directory: " + parentDir.getAbsolutePath());
//                } else {
//                    throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
//                }
//            }
//
//            // Attempt to create the file
//            if (file.createNewFile()) {
//                System.out.println("File created: " + file.getAbsolutePath());
//            } else {
//                System.out.println("File already exists: " + file.getAbsolutePath());
//            }
//        } catch (IOException e) {
//            System.out.println("An error occurred while creating the file.");
//            e.printStackTrace();
//            throw e; // Re-throw exception to handle it in the caller if needed
//        }
//
//        aws.downloadFileFromS3(keyPath, file); //download file from s3 to ec2
//
//        List<String> urlsAndOperations = parseInputFile(filePath, inputFileId); // read URLs and operations from the input file
//
//        localAppMap.put(inputFileId, urlsAndOperations.size());
//        manageWorkers(urlsAndOperations.size(), tasksPerWorker); // create correct amount of workers
//
//
//        List<String> sentMessageIds = aws.sendMessagesBatch(workersQueueUrl, urlsAndOperations);
//
////        // send url and ops to worker queue
////        for (String msg : urlsAndOperations) {
////            System.out.println("Sending message to workers Queue: " + msg);
////            aws.sendMessage(workersQueueUrl, msg);
////        }
////        System.out.println("exited for loop!!!!!");
//
//    }
//}
//
//
//    private static List<String> parseInputFile(String filePath, String inputFileId) {
//    List<String> urlsAndOperations = new ArrayList<>();
//    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
//        String line;
//        while ((line = reader.readLine()) != null) {
//            urlsAndOperations.add(line.trim()+ "\t" + inputFileId);
//        }
//    } catch (IOException e) {
//        e.printStackTrace();
//    }
//    return urlsAndOperations;
//}
//
//
//private static void manageWorkers(int messageCount, int tasksPerWorker) throws Exception {
//    int requiredWorkers = Math.min((messageCount + tasksPerWorker - 1) / tasksPerWorker, MAX_WORKERS);
//
//    List<Instance> runningWorkers = null;  // Get the count of currently running workers
//    try {
//        runningWorkers = aws.getAllInstancesWithLabel(AWS.Label.Worker);
//    } catch (InterruptedException e) {
//        throw new RuntimeException(e);
//    }
//    int currentWorkerCount = runningWorkers.size();
//
//    if (currentWorkerCount < requiredWorkers) {
//        int workersToStart = requiredWorkers - currentWorkerCount;
//        System.out.printf("Starting %d new workers...\n", workersToStart);
//        String filePath = ManagerOld.class.getClassLoader()
//                .getResource("worker-bootstrap.sh")
//                .getPath();
//        File file = new File(filePath);
//        if (!file.exists()) {
//            System.err.println("File does not exist: " + filePath);
//            return;
//        }
//        aws.uploadFileToS3(aws.getScriptPath(AWS.Label.Worker), file);
//        String script =  """
//                    #!/bin/bash
//                    aws s3 cp s3://yuval-hagar-best-bucket/worker-script/worker-bootstrap.sh /tmp/worker-bootstrap.sh
//                    chmod +x /tmp/worker-bootstrap.sh
//                    /tmp/worker-bootstrap.sh
//                    """;
//        //aws.createEC2WithLimit(script, "Worker", workersToStart);
//   }
//}
//
//
//    private static void processResponsesMessage(List<Message> responseList) {
//        for (Message response : responseList) {
//            String messageBody = response.body();
//            System.out.println("Received response: " + messageBody);
//
//            // Split the response message by tab separator (\t)
//            String[] parts = messageBody.split("\t");
//
//            // Ensure that the response message has the correct number of parts
//            if (parts.length == 4) {
//                String operation = parts[0];
//                String pdfUrl = parts[1];
//                String s3ResultsPath = parts[2];
//                String fileId = parts[3];
//
//                // Define the local file for this application
//                String localFilePath = System.getProperty("user.dir") + File.separator + "responses" + File.separator + fileId + ".txt";
//                File localFile = new File(localFilePath);
//
//                // Ensure the directory exists
//                File parentDir = localFile.getParentFile();
//                if (!parentDir.exists() && !parentDir.mkdirs()) {
//                    throw new RuntimeException("Failed to create directory for local files: " + parentDir.getAbsolutePath());
//                }
//
//                // Append the response data to the file
//                String responseData = String.format("%s\t%s\t%s", operation, pdfUrl, s3ResultsPath);
//                try (BufferedWriter writer = new BufferedWriter(new FileWriter(localFile, true))) {
//                    writer.write(responseData);
//                    writer.newLine();
//                } catch (IOException e) {
//                    System.err.println("Error writing to local file: " + localFilePath);
//                    e.printStackTrace();
//                }
//
//                if (localAppMap.containsKey(fileId)) {
//                    localAppMap.compute(fileId, (key, value) -> (value == null || value <= 0) ? 0 : value - 1);
//
//                    if (localAppMap.get(fileId) == 0) {
//                        // Generate and upload the summary file
//                        String summaryFileKey = generateSummaryFile(fileId);
//                        System.out.println("Summary file created and uploaded: " + summaryFileKey);
//                        int summaryNum = (fileId.hashCode() & Integer.MAX_VALUE) % aws.getSummaryLimit() + 1;
//                        String summaryQueueUrl = aws.getQueueUrl("summaryQueue_" + summaryNum);
//                        aws.sendMessageWithId(summaryQueueUrl, summaryFileKey, fileId);
//                        System.out.println("Summary file sent to: " + summaryQueueUrl + " with fileId: " + fileId);
//                    }
//                }
//
//            } else {
//                System.err.println("Invalid message format: " + messageBody);
//            }
//        }
//    }
//
////private static void processResponsesMessage(List<Message> responseList){
////    for(Message response : responseList) {
////        String messageBody = response.body();
////        System.out.println("Received response: " + messageBody);
////
////        // Split the response message by tab separator (\t)
////        String[] parts = messageBody.split("\t");
////
////        // Ensure that the response message has the correct number of parts
////        if (parts.length == 4) {
////            String operation = parts[0];
////            String pdfUrl = parts[1];
////            String s3ResultsPath = parts[2];
////            String fileId = parts[3];
////
////            // Generate a unique key for the individual response
////            String responseFileKey = aws.getResponsesS3Name() + fileId + "/" + response.messageId() + ".txt";
////            String responseData = String.format("%s\t%s\t%s", operation, pdfUrl, s3ResultsPath);
////            File tempFile = createTempFile(responseData);
////            // Upload the response to S3
////            try {
////                aws.uploadFileToS3(responseFileKey, tempFile);
////            } catch (Exception e) {
////                throw new RuntimeException(e);
////            }
////            tempFile.delete();
////
////            int cur = localAppMap.get(fileId) - 1;
////            if (cur == 0) {
////                String summaryFileKey = generateSummaryFile(fileId);
////                System.out.println("Summary file created: " + summaryFileKey);
////                int summaryNum = (fileId.hashCode() & Integer.MAX_VALUE) % aws.getSummaryLimit() + 1;
////                String summaryQueueUrl = aws.getQueueUrl("summaryQueue_" + summaryNum);
////                aws.sendMessageWithId(summaryQueueUrl, summaryFileKey, fileId);
////                System.out.println("Summary file sent to: summaryQueue_" + summaryNum + " with  fileId: " + fileId);
////            }
////            localAppMap.put(fileId,cur);
////        }
////
////        else {
////            System.err.println("Invalid message format: " + messageBody);
////        }
////}
////}
//private static File createTempFile(String content) {
//    File tempFile = null;
//    try {
//        tempFile = File.createTempFile("response", ".txt"); // Create a temporary file
//        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) { // Write the content to the temporary file
//            writer.write(content);
//        }
//    } catch (IOException e) {
//        e.printStackTrace();
//        throw new RuntimeException("Failed to create temporary file for upload");
//    }
//    return tempFile;
//}
//
//
////private static String generateSummaryFile(String inputFileId) {
////    String responseDirKey = aws.getResponsesS3Name() + inputFileId + "/";
////    String summaryFileKey = aws.getSummariesS3Name() + inputFileId + ".txt";
////
////    // Ensure the output-files directory exists
////    String outputDirPath = System.getProperty("user.dir") + File.separator + "output-files";
////    File outputDir = new File(outputDirPath);
////    if (!outputDir.exists()) {
////        if (!outputDir.mkdirs()) {
////            throw new RuntimeException("Failed to create output-files directory: " + outputDirPath);
////        }
////    }
////
////    // Combine content into a summary file
////    StringBuilder summaryContent = new StringBuilder();
////    List<S3Object> responseFiles = aws.listFilesInS3(responseDirKey);
////
////    for (S3Object responseFile : responseFiles) {
////        String localFilePath = outputDirPath + File.separator + inputFileId;
////        File localFile = new File(localFilePath);
////        try {
////            // Create a temporary file for the response
////            if (localFile.createNewFile()) {
////                System.out.println("File created: " + localFile.getAbsolutePath());
////            } else {
////                System.out.println("File already exists: " + localFile.getAbsolutePath());
////            }
////
////            // Download the response file from S3
////            aws.downloadFileFromS3(responseFile.key(), localFile);
////
////            // Read the content and append to summary
////            String fileContent = readFileContent(localFile);
////            summaryContent.append(fileContent).append(System.lineSeparator());
////        } catch (IOException e) {
////            System.err.println("Error processing file: " + responseFile.key());
////            e.printStackTrace();
////        } finally {
////            // Delete the local file after processing
////            if (!localFile.delete()) {
////                System.err.println("Failed to delete temporary file: " + localFile.getAbsolutePath());
////            }
////        }
////    }
////
////    // Create the summary file
////    File summaryFile = new File(outputDirPath + File.separator + inputFileId + "-summary.txt");
////    try {
////        if (summaryFile.createNewFile()) {
////            try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFile))) {
////                writer.write(summaryContent.toString());
////            }
////        } else {
////            System.err.println("Summary file already exists: " + summaryFile.getAbsolutePath());
////        }
////    } catch (IOException e) {
////        System.err.println("An error occurred while creating the summary file.");
////        e.printStackTrace();
////        throw new RuntimeException("Failed to create summary file", e);
////    }
////
////    // Upload the summary file to S3
////    try {
////       aws.uploadFileToS3(summaryFileKey, summaryFile);
////       return summaryFileKey;
////    } catch (Exception e) {
////        throw new RuntimeException("Failed to upload summary file to S3", e);
////    }
////}
//
//    private static String generateSummaryFile(String inputFileId) {
//        String localFilePath = System.getProperty("user.dir") + File.separator + "responses" + File.separator + inputFileId + ".txt";
//        File localFile = new File(localFilePath);
//
//        if (!localFile.exists()) {
//            throw new RuntimeException("Local response file not found: " + localFilePath);
//        }
//
//        String summaryFileKey = aws.getSummariesS3Name() + inputFileId + ".txt";
//
//        // Upload the local file directly to S3 as the summary file
//        try {
//            aws.uploadFileToS3(summaryFileKey, localFile);
//            System.out.println("Uploaded summary file to S3: " + summaryFileKey);
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to upload summary file to S3", e);
//        }
//
//        // Delete the local file after uploading
//        if (!localFile.delete()) {
//            System.err.println("Failed to delete local file: " + localFilePath);
//        }
//
//        return summaryFileKey;
//    }
//
//
//
//
////private static String generateSummaryFile(String inputFileId) {
////    String responseDirKey = aws.getResponsesS3Name() + inputFileId + "/";
////    String summaryFileKey = aws.getSummariesS3Name() + inputFileId + ".txt";
////
////    // List all response files in S3
////    List<S3Object> responseFiles = aws.listFilesInS3(responseDirKey);
////
////    // Combine content into a summary file
////    StringBuilder summaryContent = new StringBuilder();
////    for (S3Object responseFile : responseFiles) {
////
////        // Download the response file content from S3 to a local file
////        String filePath = System.getProperty("user.dir") + File.separator + "output-files" + File.separator + inputFileId; //locally on EC2
////        File localFile = new File(filePath);
////
////        try {
////            // Attempt to create the file
////            if (localFile.createNewFile()) {
////                System.out.println("File created: " + localFile.getAbsolutePath());
////            } else {
////                System.out.println("File already exists.");
////            }
////        } catch (IOException e) {
////            // Handle potential IO exceptions
////            System.out.println("An error occurred while creating the file.");
////            e.printStackTrace();
////        }
////        aws.downloadFileFromS3(responseFile.key(),localFile);
////        String fileContent = readFileContent(localFile);
////        summaryContent.append(fileContent).append(System.lineSeparator());
////        localFile.delete();
////    }
////    // Create a new file to store the summary
////    File summaryFile = new File(System.getProperty("user.dir") + File.separator + "output-files" + File.separator + inputFileId + "-summary.txt");
////    try {
////        if (!summaryFile.exists()) {
////            summaryFile.createNewFile();
////        }
////
////        // Write the summary content to the summary file
////        try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFile))) {
////            writer.write(summaryContent.toString());
////        }
////    } catch (IOException e) {
////        System.out.println("An error occurred while creating the summary file.");
////        e.printStackTrace();
////    }
////
////    // Upload the summary file to S3
////    try {
////        return aws.uploadFileToS3(summaryFileKey, summaryFile);
////    } catch (Exception e) {
////        throw new RuntimeException(e);
////    }
////}
//
//private static String readFileContent(File file) {
//    StringBuilder content = new StringBuilder();
//    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
//        String line;
//        while ((line = reader.readLine()) != null) {
//            content.append(line).append(System.lineSeparator());
//        }
//    } catch (IOException e) {
//        e.printStackTrace();
//    }
//    return content.toString();
//}
//
//public static void terminate() {
//    while (!(localAppMap.values().stream().allMatch(count -> count == 0))) { //not all job completed
//        try {
//            Thread.sleep(5000); // Poll every 5 seconds
//            System.out.println("Waiting for all jobs to complete...");
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//    generateAllPendingSummaries(); // Create response messages for any completed jobs, if needed
//    terminateAllWorkers();
//    List<String> managerIds = null;
//    try {
//        managerIds = aws.getAllInstanceIdsWithLabel(AWS.Label.Manager);
//    } catch (InterruptedException e) {
//        throw new RuntimeException(e);
//    }
//    aws.terminateInstance(managerIds.get(0)); //terminate the Manager
//}
//
//private static void generateAllPendingSummaries() {
//    System.out.println("Generating pending summaries...");
//    for (String inputFileId : localAppMap.keySet()) {
//        if (localAppMap.get(inputFileId) == 0) {
//            generateSummaryFile(inputFileId); // Generate the summary for completed jobs
//        }
//    }
//}
//
//private static void terminateAllWorkers() {
//    System.out.println("Terminating all worker instances...");
//    List<String> workerIds = null;
//    try {
//        workerIds = aws.getAllInstanceIdsWithLabel(AWS.Label.Worker);
//    } catch (InterruptedException e) {
//        throw new RuntimeException(e);
//    }
//    for (String id : workerIds) {
//        aws.terminateInstance(id);
//        System.out.println("Terminated worker instance: " + id);
//    }
//}
//
//public static boolean isTeminateMessage(String messageBody){
//    if (messageBody.equals("terminate")){
//        terminate = true;
//        System.out.println("Manager got terminate message");
//        return true;
//    }
//    return false;
//}
//
//}
//
//
//
//
//
//

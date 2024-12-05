import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.*;


import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
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
        args = new String[]{"/Users/hagarsamimigolan/Downloads/input-sample-1 (1).txt",
                "/Users/hagarsamimigolan/GitProject/PDF-Document-Conversion-in-the-Cloud/Local-App/target/outPutFile", "10"};
        if (args.length < 3) {
            System.out.println("Usage: LocalApplication <inputFilePath> <outputFilePath> [tasksPerWorker] [-t]");
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
            String messageBody = String.format("%s\t%s", keyPath, tasksPerWorker);
            String msgId = aws.sendMessage(inputQueueUrl, messageBody); 

            // "subscribe" to correct summary queue
            int summaryNum = (msgId.hashCode() & Integer.MAX_VALUE) % aws.getSummaryLimit() + 1;
            summaryQueueUrl = aws.getQueueUrl("summaryQueue_" + summaryNum); 
            while (summary == null) {
                summary = aws.receiveMessageWithId(summaryQueueUrl, msgId);
            }

            // download summary from s3
            File summaryFile = new File("local-summary.txt");
            aws.downloadFileFromS3(summary.body(), summaryFile);

            // creates html output file
            summaryToHTML(summaryFile);

            // Check if we need to send a termination message
            if (terminate) {
                aws.sendMessage(inputQueueUrl, "terminate"); // Send termination message to the queue
                System.out.println("Terminate message sent.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    

    //Create Buckets, Create Queues, Upload JARs to S3
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

                // Dynamically locate manager jar file locally
                String managerJarPath = LocalApplication.class.getClassLoader()
                        .getResource("Manager-1.0-SNAPSHOT.jar")
                        .getPath();
                File managerJar = new File(managerJarPath);

                if (!managerJar.exists()) {
                    System.err.println("Manager JAR does not exist: " + managerJar.getAbsolutePath());
                    return;
                }

                // Upload manager jar to S3
                aws.uploadFileToS3(aws.getJarPath(AWS.Label.Manager), managerJar);

                // Dynamically locate worker jar file locally
                String workerJarPath = LocalApplication.class.getClassLoader()
                        .getResource("Worker-1.0-SNAPSHOT.jar")
                        .getPath();
                File workerJar = new File(workerJarPath);

                if (!workerJar.exists()) {
                    System.err.println("Worker JAR does not exist: " + workerJar.getAbsolutePath());
                    return;
                }

                // Upload the Worker JAR to S3
                aws.uploadFileToS3(aws.getJarPath(AWS.Label.Worker), workerJar);




//                // Locate the "target" directory of the current application
//                URL resource = LocalApplication.class.getProtectionDomain().getCodeSource().getLocation();
//                File targetDir = new File(resource.toURI()).getParentFile(); // Parent of current "classes" directory
//
//                // Locate Manager JAR
//                File managerJar = new File(targetDir, "../Manager/target/Manager-1.0-SNAPSHOT.jar");
//                if (!managerJar.exists()) {
//                    System.err.println("Manager JAR does not exist: " + managerJar.getAbsolutePath());
//                    return;
//                }
//                System.out.println("Manager JAR Path: " + managerJar.getAbsolutePath());
//
//                // Upload Manager JAR to S3
//                aws.uploadFileToS3(aws.getJarPath(AWS.Label.Manager), managerJar);
//
//                // Locate Worker JAR
//                File workerJar = new File(targetDir, "../Worker/target/Worker-1.0-SNAPSHOT.jar");
//                if (!workerJar.exists()) {
//                    System.err.println("Worker JAR does not exist: " + workerJar.getAbsolutePath());
//                    return;
//                }
//                System.out.println("Worker JAR Path: " + workerJar.getAbsolutePath());
//
//                // Upload Worker JAR to S3
//                aws.uploadFileToS3(aws.getJarPath(AWS.Label.Worker), workerJar);

                // Dynamically locate manager-bootstrap.sh
                String filePath = LocalApplication.class.getClassLoader()
                        .getResource("manager-bootstrap.sh")
                        .getPath();
                File file = new File(filePath);

                if (!file.exists()) {
                    System.err.println("File does not exist: " + filePath);
                    return;
                }

                // upload to S3
                aws.uploadFileToS3(aws.getScriptPath(AWS.Label.Manager), file);
                String script = """
                    #!/bin/bash
                    aws s3 cp s3://yuval-hagar-best-bucket/manager-script/manager-bootstrap.sh /tmp/manager-bootstrap.sh
                    chmod +x /tmp/manager-bootstrap.sh
                    /tmp/manager-bootstrap.sh
                    """;
                aws.createEC2(script, "Manager", 1); // create manager EC2
            }
        } catch (InterruptedException e) {
            System.err.println("Error occurred while retrieving instances: " + e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

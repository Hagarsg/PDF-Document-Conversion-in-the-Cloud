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
        args = new String[]{"/Users/hagarsamimigolan/GitProject/Yuval&Hagar/newInputFile.txt",
                "/Users/hagarsamimigolan/GitProject/Yuval&Hagar/outputFile.html", "10", "-t"};
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
                File managerBootstrapScript = getResourceFile("manager-bootstrap.sh");
                File workerBootstrapScript = getResourceFile("worker-bootstrap.sh");

                if (managerJar == null || workerJar == null || managerBootstrapScript == null || workerBootstrapScript == null) {
                    System.err.println("Required resources are missing. Aborting setup.");
                    return;
                }

                // Upload files to S3
                aws.uploadFileToS3("manager.jar", managerJar);
                aws.uploadFileToS3("worker.jar", workerJar);
                //aws.uploadFileToS3("manager-bootstrap.sh", managerBootstrapScript);
                //aws.uploadFileToS3("worker-bootstrap.sh", workerBootstrapScript);

                // EC2 bootstrap script for Manager
                String managerScript = "#!/bin/bash\n" +
                        "set -e # Exit immediately if a command exits with a non-zero status\n" +
                        "\n" +
                        "# Update package lists\n" +
                        "sudo yum update -y\n" +
                        "\n" +
                        "# Check if Java is installed\n" +
                        "if ! java -version &>/dev/null; then\n" +
                        "#!/bin/bash\n" +
                        "set -e\n" +
                        "sudo yum update -y\n" +
                        "if ! java -version &>/dev/null; then\n" +
                        "    sudo yum install -y java-1.8.0-openjdk\n" +
                        "else\n" +
                        "    echo \"Java is already installed. Skipping installation.\"\n" +
                        "fi\n" +
                        "sudo yum install -y aws-cli\n" +
                        "java -version\n" +
                        "S3_BUCKET=\"yuval-hagar-best-bucket\"\n" +
                        "S3_MANAGER_JAR=\"manager.jar\"\n" +
                        "LOCAL_APP_DIR=\"/home/ec2-user/app\"\n" +
                        "mkdir -p $LOCAL_APP_DIR\n" +
                        "cd $LOCAL_APP_DIR\n" +
                        "aws s3 cp s3://$S3_BUCKET/$S3_MANAGER_JAR ./manager.jar\n" +
                        "chmod +x manager.jar\n" +
                        "java -jar manager.jar > manager.log 2>&1 &";
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



    //Create Buckets, Create Queues, Upload JARs to S3
//    private static void setup() {
//        try {
//            List<Instance> list = aws.getAllInstancesWithLabel(AWS.Label.Manager);
//            if (list.isEmpty()) { // if manager is not active
//                aws.createBucketIfNotExists(aws.getBucketName());
//                inputQueueUrl = aws.createQueue(aws.getInputQueueName());
//                for (int i = 1; i <= aws.getSummaryLimit(); i++) {
//                    String name = "summaryQueue_" + i;
//                    aws.createQueue(name);
//                }
//
//                // Dynamically locate manager jar file locally
//                String managerJarPath = LocalApplication.class.getClassLoader()
//                        .getResource("Manager-1.0-SNAPSHOT.jar")
//                        .getPath();
//                File managerJar = new File(managerJarPath);
//
//                if (!managerJar.exists()) {
//                    System.err.println("Manager JAR does not exist: " + managerJar.getAbsolutePath());
//                    return;
//                }
//
//                // Upload manager jar to S3
//                aws.uploadFileToS3(aws.getJarPath(AWS.Label.Manager), managerJar);
//
//                // Dynamically locate worker jar file locally
//                String workerJarPath = LocalApplication.class.getClassLoader()
//                        .getResource("Worker-1.0-SNAPSHOT.jar")
//                        .getPath();
//                File workerJar = new File(workerJarPath);
//
//                if (!workerJar.exists()) {
//                    System.err.println("Worker JAR does not exist: " + workerJar.getAbsolutePath());
//                    return;
//                }
//
//                // Upload the Worker JAR to S3
//                aws.uploadFileToS3(aws.getJarPath(AWS.Label.Worker), workerJar);
//
//                // Dynamically locate manager-bootstrap.sh
//                String filePath = LocalApplication.class.getClassLoader()
//                        .getResource("manager-bootstrap.sh")
//                        .getPath();
//                File file = new File(filePath);
//
//                if (!file.exists()) {
//                    System.err.println("File does not exist: " + filePath);
//                    return;
//                }
//
//                // upload to S3
//                aws.uploadFileToS3(aws.getScriptPath(AWS.Label.Manager), file);
//                String script = "#!/bin/bash" +
//                    "aws s3 cp s3://yuval-hagar-best-bucket/manager-script/manager-bootstrap.sh /tmp/manager-bootstrap.sh"
//                     + "chmod +x /tmp/manager-bootstrap.sh"
//                     + "/tmp/manager-bootstrap.sh";
//                aws.createEC2(script, "Manager", 1); // create manager EC2
//            }
//        } catch (InterruptedException e) {
//            System.err.println("Error occurred while retrieving instances: " + e.getMessage());
//            Thread.currentThread().interrupt();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

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


import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class AWS {

    public final String IMAGE_AMI = "ami-04222fdf1b349e78e";
    public Region region1 = Region.US_WEST_2;
    public Region region2 = Region.US_EAST_1;
    private final int ec2RegionLimit = 9;
    private static volatile AWS instance;
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    private final String bucketName = "yuval-hagar-best-bucket";
    private final String inputQueueName = "inputQueue";
    private final String workerQueueName = "workerQueue";
    private final String responsesQueueName = "responsesQueue";
    private final String inputFileS3Name = "input-files/";
    private final String responsesS3Name = "responses/";
    private final String resultsS3Name = "results/";
    private final String summariesS3Name = "summaries/";
    private final String managerScriptPath = "manager-script";
    private final String workerScriptPath = "worker-script";
    private final String managerJarPath = "manager.jar";
    private final String workerJarPath = "worker.jar";
    private final int visibilityTimeoutSeconds = 10;
//    private final int workerVisibilityTO = 10;
//    private final int responseVisibilityTO = 10;
//    private final int summaryVisibilityTO = 10;




    private final int summaryLimit = 10;


    public static AWS getInstance() {
        if (instance == null) {
            synchronized (AWS.class) {
                if (instance == null) {
                    instance = new AWS();
                }
            }
        }
        return instance;
    }


    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build(); // see if matters what region
        ec2 = Ec2Client.builder().region(region2).build();
    }


    //////////////////////////////////////////  EC2

//    // EC2
//    public String createEC2(String script, String tagName, int numberOfInstances) {
//        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
//                .instanceType(InstanceType.M4_LARGE)
//                .imageId(IMAGE_AMI)
//                .maxCount(numberOfInstances)
//                .minCount(1)
//                .keyName("vockey")
//                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
//                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
//                .build();
//
//
//        RunInstancesResponse response = ec2.runInstances(runRequest);
//
//        String instanceId = response.instances().get(0).instanceId();
//
//        Tag tag = Tag.builder()
//                .key("Name")
//                .value(tagName)
//                .build();
//
//        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
//                .resources(instanceId)
//                .tags(tag)
//                .build();
//
//        try {
//            ec2.createTags(tagRequest);
//            System.out.printf(
//                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
//                    instanceId, IMAGE_AMI);
//
//        } catch (Ec2Exception e) {
//            System.err.println("[ERROR] " + e.getMessage());
//            System.exit(1);
//        }
//        return instanceId;
//    }

    public String createEC2(String script, String tagName, int numberOfInstances) {
        // Remove the keyName as we're not using a key pair
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(IMAGE_AMI)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("Hagar")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, IMAGE_AMI);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public void runInstanceFromAMI(String ami) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(InstanceType.T2_MICRO)
                .minCount(1)
                .maxCount(5) // todo decide what to put here
                .build();

        // Launch the instance
        try {
            ec2.runInstances(runInstancesRequest);
        } catch (Ec2Exception e) {
            System.err.println("Failed to launch instance: " + e.awsErrorDetails().errorMessage());
        }
    }

    public RunInstancesResponse runInstanceFromAmiWithScript(String ami, InstanceType instanceType, int min, int max, String script) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(instanceType)
                .minCount(min)
                .maxCount(max)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                // @ADD security features
                .build();

        // Launch the instance
        try {
            return ec2.runInstances(runInstancesRequest);
        } catch (Ec2Exception e) {
            System.err.println("Failed to launch instance: " + e.awsErrorDetails().errorMessage());
            throw new RuntimeException("Could not run instance", e);
        }
    }

    public List<Instance> getAllInstances() {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();

        DescribeInstancesResponse describeInstancesResponse = null;
        try {
            describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);
        } catch (Ec2Exception e) {
            System.err.println("Failed to describe instances: " + e.awsErrorDetails().errorMessage());
            throw new RuntimeException("Could not retrieve instances", e);
        }


        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public List<Instance> getAllInstancesWithLabel(Label label) throws InterruptedException {
        DescribeInstancesRequest describeInstancesRequest =
                DescribeInstancesRequest.builder()
                        .filters(Filter.builder()
                                .name("tag:Label")
                                .values(label.toString())
                                .build())
                        .build();

        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public List<String> getAllInstanceIdsWithLabel(Label label) throws InterruptedException {
        DescribeInstancesRequest describeInstancesRequest =
                DescribeInstancesRequest.builder()
                        .filters(Filter.builder()
                                .name("tag:Label")
                                .values(label.toString())
                                .build())
                        .build();

        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .map(Instance::instanceId) // Extract only the instance ID
                .toList();
    }

    public void terminateInstance(String instanceId) {
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        // Terminate the instance
        try {
            ec2.terminateInstances(terminateRequest);
        }  catch (Ec2Exception e) {
            System.err.println("Failed to launch instance: " + e.awsErrorDetails().errorMessage());
            throw new RuntimeException("Could not run instance", e);
        }

        System.out.println("Terminated instance: " + instanceId);
    }
    /// ///////// new method for ec2

    public List<String> createEC2WithLimit(String script, String tagName, int requestedInstances) {
        List<String> createdInstanceIds = new ArrayList<>();

        synchronized (this) { // precaution
            // Get all instances in the region and count the running ones
            int runningInstances = (int) getAllInstances().stream()
                    .filter(instance -> instance.state().name().equals(InstanceStateName.RUNNING))
                    .count();

            int availableCapacity = ec2RegionLimit - runningInstances;

            if (availableCapacity > 0) {
                int instancesToCreate = Math.min(availableCapacity, requestedInstances);

                // Create EC2 instances
                RunInstancesRequest runRequest = RunInstancesRequest.builder()
                        .instanceType(InstanceType.M4_LARGE)
                        .imageId(IMAGE_AMI)
                        .maxCount(instancesToCreate)
                        .minCount(1)
                        .keyName("Hagar")
                        .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                        .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                        .build();

                RunInstancesResponse response = ec2.runInstances(runRequest);

                // Collect instance IDs and tag them
                createdInstanceIds = response.instances().stream()
                        .map(instance -> {
                            String instanceId = instance.instanceId();

                            Tag tag = Tag.builder()
                                    .key("Name")
                                    .value(tagName)
                                    .build();

                            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                                    .resources(instanceId)
                                    .tags(tag)
                                    .build();

                            try {
                                ec2.createTags(tagRequest);
                                System.out.printf(
                                        "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                                        instanceId, IMAGE_AMI);
                            } catch (Ec2Exception e) {
                                System.err.println("[ERROR] " + e.getMessage());
                            }

                            return instanceId;
                        })
                        .toList();
            }
        }

        if (createdInstanceIds.isEmpty()) {
            throw new RuntimeException("Cannot create instances: Region is at maximum capacity.");
        }

        return createdInstanceIds;
    }



    ////////////////////////////// S3

    public String uploadFileToS3(String keyPath, File file) throws Exception {
        System.out.printf("Start upload: %s, to S3\n", file.getName());

        PutObjectRequest req =
                PutObjectRequest.builder()

                        .bucket(bucketName)
                        .key(keyPath)
                        .build();

        s3.putObject(req, file.toPath()); // we don't need to check if the file exist already
        // Return the S3 path of the uploaded file
        return "s3://" + bucketName + "/" + keyPath;
    }

    public String createEmptyFileInS3(String keyPath) throws Exception {
        PutObjectRequest req =
                PutObjectRequest.builder()

                        .bucket(bucketName)
                        .key(keyPath)
                        .build();
        return "s3://" + bucketName + "/" + keyPath;
    }

    public void downloadFileFromS3(String keyPath, File outputFile) {
        System.out.println("Start downloading file " + keyPath + " to " + outputFile.getPath());

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        try {
            // Retrieve the object as bytes from S3
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            byte[] data = objectBytes.asByteArray();

            try (OutputStream os = new FileOutputStream(outputFile)) {
                os.write(data);
                System.out.println("Successfully downloaded and saved the file from S3.");
            }
        } catch (S3Exception e) {
            // Handle S3-specific exceptions
            System.err.println("Failed to download file from S3: " + e.awsErrorDetails().errorMessage());
            e.printStackTrace();
        } catch (IOException e) {
            // Handle file writing errors
            System.err.println("Error writing to file: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            // Catch-all for unexpected exceptions
            System.err.println("Unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public SdkIterable<S3Object> listObjectsInBucket(String bucketName) {
        // Build the list objects request
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(1)
                .build();

        ListObjectsV2Iterable listRes = null;
        try {
            listRes = s3.listObjectsV2Paginator(listReq);
        } catch (S3Exception ignored) {
        }
        // Process response pages
        listRes.stream()
                .flatMap(r -> r.contents().stream())
                .forEach(content -> System.out.println(" Key: " + content.key() + " size = " + content.size()));

        return listRes.contents();
    }


    public List<S3Object> listFilesInS3(String prefix) { //new
        // Create a request to list objects with the specified prefix
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix) // Directory path (e.g., "responses/inputFileId/")
                .build();

        // Execute the request and get the response
        ListObjectsV2Response listResponse = s3.listObjectsV2(listRequest);

        // Return the list of S3 objects
        return listResponse.contents();
    }

    public void deleteEmptyBucket(String bucketName) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
        try {
            s3.deleteBucket(deleteBucketRequest);
        } catch (S3Exception ignored) {
        }
    }

    public void deleteAllObjectsFromBucket(String bucketName) {
        SdkIterable<S3Object> contents = listObjectsInBucket(bucketName);

        Collection<ObjectIdentifier> keys = contents.stream()
                .map(content ->
                        ObjectIdentifier.builder()
                                .key(content.key())
                                .build())
                .toList();

        Delete del = Delete.builder().objects(keys).build();

        DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(del)
                .build();

        try {
            s3.deleteObjects(multiObjectDeleteRequest);
        } catch (S3Exception ignored) {
        }
    }

    public void deleteBucket(String bucketName) {
        deleteAllObjectsFromBucket(bucketName);
        deleteEmptyBucket(bucketName);
    }

    //////////////////////////////////////////////SQS

    /**
     * @param queueName
     * @return queueUrl
     */
    public String createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse create_result = null;
        try {
            create_result = sqs.createQueue(request);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

        assert create_result != null;
        String queueUrl = create_result.queueUrl();
        System.out.println("Created queue '" + queueName + "', queue URL: " + queueUrl);
        return queueUrl;
    }

    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest req =
                DeleteQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .build();

        try {
            sqs.deleteQueue(req);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = null;
        try {
            queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        System.out.println("Queue URL: " + queueUrl);
        return queueUrl;
    }

    public int getQueueSize(String queueUrl) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED
                )
                .build();

        GetQueueAttributesResponse queueAttributesResponse = null;
        try {
            queueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
    }


    public String sendMessage(String queueUrl, String messageBody) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        SendMessageResponse sendMessageResponse = sqs.sendMessage(sendMessageRequest);
        String messageId = sendMessageResponse.messageId();
        return messageId;
    }

    public String sendMessageWithId(String queueUrl, String messageBody, String fileId) {
        MessageAttributeValue fileIdAttribute = MessageAttributeValue.builder()
                .stringValue(fileId)
                .dataType("String")
                .build();

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageAttributes(Map.of("FileId", fileIdAttribute)) // Add the fileId attribute
                .build();

        SendMessageResponse sendMessageResponse = sqs.sendMessage(sendMessageRequest);

        String messageId = sendMessageResponse.messageId();
        System.out.println("Message sent with MessageId: " + messageId + " and FileId: " + fileId);
        return messageId;
    }


    public List<String> sendMessagesBatch(String queueUrl, List<String> messages) {
        // Limit batch size to 10 messages, as SQS supports a max of 10 per batch
        final int batchSize = 10;
        List<String> sentMessageIds = new ArrayList<>();

        for (int i = 0; i < messages.size(); i += batchSize) {
            // Create a sublist for the current batch
            List<String> batch = messages.subList(i, Math.min(i + batchSize, messages.size()));

            // Create batch request entries
            List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
            for (int j = 0; j < batch.size(); j++) {
                entries.add(SendMessageBatchRequestEntry.builder()
                        .id("msg-" + (i + j)) // Unique ID for each message in the batch
                        .messageBody(batch.get(j))
                        .build());
            }

            // Build and send the batch request
            SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build();

            SendMessageBatchResponse response = sqs.sendMessageBatch(batchRequest);

            // Collect the message IDs of successfully sent messages
            response.successful().forEach(success -> sentMessageIds.add(success.id()));

            // Handle failed messages
            response.failed().forEach(failure -> {
                System.err.printf("Failed to send message ID %s: %s%n", failure.id(), failure.message());
            });
            // Log after sending each batch
            System.out.printf("Batch sent: %d messages (Batch %d to %d)%n",
                    batch.size(), i + 1, Math.min(i + batchSize, messages.size()));
        }

        System.out.println("All messages sent in batches!");
        return sentMessageIds;
    }


    public List<Message> receiveMessages(String queueUrl) {
        // Build the ReceiveMessageRequest
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10) // Batch fetch up to 10 messages
                .visibilityTimeout(visibilityTimeoutSeconds) // Set visibility timeout in seconds
                .build();

        // Use the SQS client to receive messages
        return sqs.receiveMessage(receiveMessageRequest).messages();
    }


    public Message receiveMessageWithId(String queueUrl, String appFileId) {
        // Build the ReceiveMessageRequest
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1) // Fetch one message at a time
                .visibilityTimeout(visibilityTimeoutSeconds)
                .waitTimeSeconds(20)
                .messageAttributeNames("All") // Include all message attributes
                .build();

        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(receiveMessageRequest);
        List<Message> messages = receiveMessageResponse.messages();

        if (messages.isEmpty()) {
            System.out.println("No messages available.");
            return null;
        }

        Message message = messages.get(0);

        String fileId = message.messageAttributes().getOrDefault("FileId", null) != null
                ? message.messageAttributes().get("FileId").stringValue()
                : null;

        if (fileId != null && fileId.equals(appFileId)) {
            System.out.println("Processing message: " + message.body());
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
            return message;
        } else {
            System.out.println("Returning message to the queue. FileId: " + fileId);
            sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .visibilityTimeout(0)
                    .build());
            return null;
        }
    }




    public void deleteMessages(String queueUrl, List<Message> messages) {
        for (Message m : messages) {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(m.receiptHandle()) // Correct method for SDK v2
                    .build());
        }
    }

//    public void deleteMessage(String queueUrl, Message message) {
//        sqs.deleteMessage(DeleteMessageRequest.builder()
//                .queueUrl(queueUrl)
//                .receiptHandle(message.receiptHandle()) // Correct method for SDK v2
//                .build());
//    }

    public void deleteMessage(String queueUrl, Message message) {
        String receiptHandle = message.receiptHandle();
        System.out.println("Attempting to delete message with ReceiptHandle: " + receiptHandle + " from queue: " + queueUrl);

        try {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build());
            System.out.println("Successfully deleted message with ReceiptHandle: " + receiptHandle + " from queue: " + queueUrl);
        } catch (Exception e) {
            System.out.println("Failed to delete message with ReceiptHandle: " + receiptHandle + " from queue: " + queueUrl
                    + ". Error: " + e.getMessage());
        }
    }
    /////////// Getter Methods


    public int getSummaryLimit(){
        return summaryLimit;
    }

    public String getBucketName(){
        return bucketName;
    }

    public String getInputQueueName() {
        return inputQueueName;
    }

    public String getWorkerQueueName() {
        return workerQueueName;
    }

    public String getResponsesQueueName() {
        return responsesQueueName;
    }

    public String getInputFileS3Name() {
        return inputFileS3Name;
    }

    public String getResultsS3Name() {
        return resultsS3Name;
    }

    public String getSummariesS3Name() {
        return summariesS3Name;
    }

    public String getResponsesS3Name(){
        return responsesS3Name;
    }


    public String getScriptPath(Label label) {
        if (label == Label.Manager) {
            return managerScriptPath;
        } else if (label == Label.Worker) {
            return workerScriptPath;
        } else {
            return "Invalid Label"; // maybe throw exception
        }
    }

    public String getJarPath(Label label) {
        if (label == Label.Manager) {
            return managerJarPath;
        } else if (label == Label.Worker) {
            return workerJarPath;
        } else {
            return "Invalid Label"; // maybe throw exception
        }
    }



    ///////////////////////

    public enum Label {
        Manager,
        Worker
    }
}

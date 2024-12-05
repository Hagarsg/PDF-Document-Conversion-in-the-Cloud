
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

    public final String IMAGE_AMI = "ami-0e1b037ab311948d5";
    public Region region1 = Region.US_WEST_2;
    public Region region2 = Region.US_EAST_1;
    private final int ec2RegionLimit = 9;

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




    private static AWS instance = null;
    private final int summaryLimit = 10;



    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region1).build();
    }

    public static AWS getInstance() {
        if (instance == null) {
            instance = new AWS();
        }
        return instance;
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
    /// ///////// new methods for ec2

    public List<String> createEC2WithLimit(String script, String tagName, int requestedInstances) {
        List<String> createdInstanceIds = new ArrayList<>();

        synchronized (this) { // Ensures thread safety
            // Check capacity in region 1
            int region1Available = ec2RegionLimit - countRunningInstances(region1);

            if (region1Available > 0) {
                int instancesToCreate = Math.min(region1Available, requestedInstances);
                createdInstanceIds.addAll(createEC2InRegion(region1, script, tagName, instancesToCreate));
                requestedInstances -= instancesToCreate;
            }

            // Check capacity in region 2 if more instances are needed
            if (requestedInstances > 0) {
                int region2Available = ec2RegionLimit - countRunningInstances(region2);
                if (region2Available > 0) {
                    int instancesToCreate = Math.min(region2Available, requestedInstances);
                    createdInstanceIds.addAll(createEC2InRegion(region2, script, tagName, instancesToCreate));
                }
            }
        }

        if (createdInstanceIds.isEmpty()) {
            throw new RuntimeException("Cannot create instances: Both regions are at maximum capacity.");
        }

        return createdInstanceIds;
    }

    // Count running instances in a specific region
    private int countRunningInstances(Region region) {
        Ec2Client ec2Client = Ec2Client.builder().region(region).build();
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .filters(Filter.builder()
                        .name("instance-state-name")
                        .values("running")
                        .build())
                .build();

        DescribeInstancesResponse response = ec2Client.describeInstances(describeInstancesRequest);

        return response.reservations().stream()
                .mapToInt(r -> r.instances().size())
                .sum();
    }

    // Create EC2 instances in a specific region
    private List<String> createEC2InRegion(Region region, String script, String tagName, int numberOfInstances) {
        Ec2Client ec2Client = Ec2Client.builder().region(region).build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(IMAGE_AMI)
                .maxCount(numberOfInstances)
                .minCount(1)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .build();

        RunInstancesResponse response = ec2Client.runInstances(runRequest);

        List<String> instanceIds = response.instances().stream()
                .map(Instance::instanceId)
                .toList();

        // Tag each created instance
        for (String instanceId : instanceIds) {
            Tag tag = Tag.builder()
                    .key("Name")
                    .value(tagName)
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            ec2Client.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s in region %s\n",
                    instanceId, IMAGE_AMI, region);
        }

        return instanceIds;
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





//    public void sendMessageBatches(String queueUrl, List<String> messages, String messageId) { // check if needs to be synchronized
//
//        Iterator<String> msgIter = messages.iterator();
//        while (msgIter.hasNext()) {
//            List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
//
//            // create batches of 10 entries (aws limitations)
//            for (int i = 1; msgIter.hasNext() && i <= 10; i++ ) {
//                entries.add(new SendMessageBatchRequestEntry("msg_" + i, msgIter.next()));
//            }
//
//            SendMessageBatchRequest batchRequest = new SendMessageBatchRequest()
//                .withQueueUrl(queueUrl)
//                .withEntries(entries);
//
//            // send batch
//            sqs.sendMessageBatch(batchRequest);
//        }
//    }


    public List<Message> receiveMessages(String queueUrl) {
        // Build the ReceiveMessageRequest
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .build();

        // Use the SQS client to receive messages
        return sqs.receiveMessage(receiveMessageRequest).messages();
    }


    public Message receiveMessageWithId(String queueUrl, String appFileId) {
        // Build the ReceiveMessageRequest
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1) // Fetch one message at a time
                .waitTimeSeconds(10)
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

    public void deleteMessage(String queueUrl, Message message) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle()) // Correct method for SDK v2
                .build());
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

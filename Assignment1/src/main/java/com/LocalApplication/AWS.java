package com.LocalApplication;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class AWS
{
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    public static Region region = Region.US_EAST_1;
    private static final AWS instance = new AWS();

    private AWS()
    {
        s3 = S3Client.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();
        ec2 = Ec2Client.builder().region(region).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    public void createBucketIfNotExists(String bucket)
    {
        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .objectOwnership(ObjectOwnership.BUCKET_OWNER_PREFERRED)
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucket)
                    .build());

            PublicAccessBlockConfiguration publicAccessBlockConfiguration = PublicAccessBlockConfiguration.builder()
                    .blockPublicAcls(false)
                    .ignorePublicAcls(false)
                    .blockPublicPolicy(false)
                    .restrictPublicBuckets(false)
                    .build();
            PutPublicAccessBlockRequest putPublicAccessBlockRequest = PutPublicAccessBlockRequest.builder()
                    .bucket(bucket)
                    .publicAccessBlockConfiguration(publicAccessBlockConfiguration)
                    .build();

            s3.putPublicAccessBlock(putPublicAccessBlockRequest);
        }
        catch (S3Exception e) { System.out.println(e.getMessage()); }
    }

    public void uploadObjectToBucket(String bucket, String key, String path)
    {
        try
        {
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucket)
                    .acl(ObjectCannedACL.PUBLIC_READ)
                    .key(key)
                    .build();

            s3.putObject(putOb, RequestBody.fromFile(new File(path)));

        }
        catch (S3Exception e) { System.out.println(e.getMessage()); }
    }

    public void downloadObjectFromBucket(String bucket, String key, String path)
    {
        try
        {
            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            ResponseBytes<GetObjectResponse> responseResponseBytes = s3.getObjectAsBytes(objectRequest);

            byte[] data = responseResponseBytes.asByteArray();
            java.io.File myFile = new java.io.File(path);
            OutputStream os = new FileOutputStream(myFile);

            os.write(data);
            os.close();
        }
        catch (FileNotFoundException e) { e.printStackTrace(); }
        catch (IOException e) { e.printStackTrace(); }
    }

    public void emptyBucket(String bucket)
    {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucket)
                .build();

        ListObjectsV2Response listResponse = s3.listObjectsV2(listRequest);

        List<ObjectIdentifier> objectsToDelete = new ArrayList<>();
        for (S3Object object : listResponse.contents()) {
            objectsToDelete.add(ObjectIdentifier.builder().key(object.key()).build());
        }

        DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder().objects(objectsToDelete).build())
                .build();

        s3.deleteObjects(deleteRequest);
    }

    public void createQueueIfNotExists(String queue)
    {
        try
        {
            CreateQueueRequest createStandardQueueRequest = CreateQueueRequest.builder()
                    .queueName(queue)
                    .build();

            sqs.createQueue(createStandardQueueRequest);
        }
        catch (SqsException e) { System.out.println(e.getMessage()); }
    }

    public String getQueueURL(String queue)
    {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queue)
                .build();

        GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(getQueueUrlRequest);
        return getQueueUrlResponse.queueUrl();
    }

    public void sendMessage(String queue, String message)
    {
        SendMessageRequest sendMessageStandardQueue = SendMessageRequest.builder()
                .queueUrl(getQueueURL(queue))
                .messageBody(message)
                .build();

        sqs.sendMessage(sendMessageStandardQueue);
    }

    public Message receiveMessage(String queue)
    {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQueueURL(queue))
                .maxNumberOfMessages(1)
                .build();

        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(receiveMessageRequest);
        List<Message> messages = receiveMessageResponse.messages();

        if (messages.isEmpty()) { return null; }
        else { return messages.getFirst(); }
    }

    public void deleteMessage(String queue, Message message)
    {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(getQueueURL(queue))
                .receiptHandle(message.receiptHandle())
                .build();

        sqs.deleteMessage(deleteMessageRequest);
    }

    public void purgeQueue(String queue)
    {
        PurgeQueueRequest purgeRequest = PurgeQueueRequest.builder()
                .queueUrl(getQueueURL(queue))
                .build();

        sqs.purgeQueue(purgeRequest);
    }

    public String createInstance(String instance)
    {
        String userDataScript =
                "Content-Type: multipart/mixed; boundary=\"//\"\n" +
                "MIME-Version: 1.0\n" +
                "\n" +
                "--//\n" +
                "Content-Type: text/cloud-config; charset=\"us-ascii\"\n" +
                "MIME-Version: 1.0\n" +
                "Content-Transfer-Encoding: 7bit\n" +
                "Content-Disposition: attachment; filename=\"cloud-config.txt\"\n" +
                "\n" +
                "#cloud-config\n" +
                "cloud_final_modules:\n" +
                "- [scripts-user, always]\n" +
                "\n" +
                "--//\n" +
                "Content-Type: text/x-shellscript; charset=\"us-ascii\"\n" +
                "MIME-Version: 1.0\n" +
                "Content-Transfer-Encoding: 7bit\n" +
                "Content-Disposition: attachment; filename=\"userdata.txt\"" + "\n" +

                "#!/bin/bash\n" +
                "echo 'Y' | sudo apt-get update\n" +
                "echo 'Y' | sudo apt install openjdk-21-jdk\n" +
                "cd /usr/bin\n" +
                "sudo mkdir managerPath\n" +
                "cd managerPath\n" +
                "sudo wget https://bucket-sarcasm-analysis-eng.s3.amazonaws.com/manager/manager.jar -O manager.jar\n" +
                "java -jar manager.jar\n" +
                "sudo shutdown -h now\n";

        String base64UserData = Base64.getEncoder().encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8));

        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId("ami-080e1f13689e07408")
                .instanceType(InstanceType.M4_LARGE)
                .minCount(1)
                .maxCount(1)
                .userData(base64UserData)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .instanceInitiatedShutdownBehavior(ShutdownBehavior.TERMINATE)
                .build();

        RunInstancesResponse runInstancesResponse = ec2.runInstances(runInstancesRequest);
        Instance instanceObject = runInstancesResponse.instances().getFirst();
        String instanceId = instanceObject.instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(instance)
                .build();

        CreateTagsRequest createTagsRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        ec2.createTags(createTagsRequest);

        return instanceId;
    }

    public boolean isInstanceActive(String instanceId)
    {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        for (Reservation reservation : describeInstancesResponse.reservations())
        {
            for (Instance instance : reservation.instances())
            {
                InstanceState instanceState = instance.state();
                if (instanceState.name() == InstanceStateName.RUNNING) { return true; }
            }
        }

        return false;
    }

    public void startInstance(String instanceId)
    {
        StartInstancesRequest startInstancesRequest = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(startInstancesRequest);
    }

    public boolean isInstanceWithTagExists(String tag)
    {
        Filter tagFilter = Filter.builder()
                .name("tag:" + "Name")
                .values(tag)
                .build();

        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(tagFilter)
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request);

        for (Reservation reservation : response.reservations())
        {
            for (Instance instance : reservation.instances()) { return true; }
        }

        return false;
    }

    public String getInstanceIdByTag(String tag)
    {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(Filter.builder()
                        .name("tag:" + "Name")
                        .values(tag)
                        .build())
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request);

        for (Reservation reservation : response.reservations())
        {
            for (Instance instance : reservation.instances()) { return instance.instanceId(); }
        }

        return null;
    }
}

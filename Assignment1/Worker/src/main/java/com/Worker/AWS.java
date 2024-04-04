package com.Worker;

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
import java.util.List;

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
        catch (S3Exception e) { e.printStackTrace(); }
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
                .visibilityTimeout(60*10)
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

    public List<S3Object> listObjects(String bucket, String key)
    {
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(key)
                .build();

        ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);
        return listObjectsResponse.contents();
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

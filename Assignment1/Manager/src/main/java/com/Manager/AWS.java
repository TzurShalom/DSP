package com.Manager;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class AWS
{
    private final SqsClient sqs;
    private final Ec2Client ec2;
    public static Region region = Region.US_EAST_1;
    private static final AWS instance = new AWS();

    private AWS()
    {
        sqs = SqsClient.builder().region(region).build();
        ec2 = Ec2Client.builder().region(region).build();
    }

    public static AWS getInstance() {
        return instance;
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

    public boolean isQueueEmpty(String queue)
    {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQueueURL(queue))
                .maxNumberOfMessages(1)
                .build();

        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(receiveMessageRequest);
        return  receiveMessageResponse.messages().isEmpty();
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
                "sudo mkdir workerPath\n" +
                "cd workerPath\n" +
                "sudo touch input.txt\n" +
                "sudo touch output.txt\n" +
                "sudo chmod 777 input.txt\n" +
                "sudo chmod 777 output.txt\n" +
                "sudo wget https://bucket-sarcasm-analysis-eng.s3.amazonaws.com/worker/worker.jar -O worker.jar\n" +
                "java -jar worker.jar\n" +
                "sudo shutdown -h now\n";

        String base64UserData = Base64.getEncoder().encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8));

        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId("ami-080e1f13689e07408")
                .instanceType(InstanceType.M4_LARGE)
                .minCount(1)
                .maxCount(1)
                .userData(base64UserData)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
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

    public void terminateInstance(String instanceId)
    {
        TerminateInstancesRequest terminateInstancesRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.terminateInstances(terminateInstancesRequest);
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

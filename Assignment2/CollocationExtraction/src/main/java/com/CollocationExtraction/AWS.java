package com.CollocationExtraction;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;


import java.io.File;
import java.util.*;

public class AWS
{
    private final EmrClient emr;
    private final S3Client s3;
    public static Region region = Region.US_EAST_1;
    private static final AWS instance = new AWS();
    private AWS()
    {
        emr = EmrClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();
    }
    public static AWS getInstance() {
        return instance;
    }

    public void start(String input, String bucket, double minPmi, double relMinPmi)
    {
        List<StepConfig> steps = new ArrayList<>();

        //--------------------------------------------------------------------------------//

        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar("s3://"+bucket+"/step1.jar")
                .mainClass("com.step1.Main")
                .args(input, "s3://"+bucket+"/output1/")
                .build();

        StepConfig stepConfig1 = StepConfig.builder()
                .name("step 1")
                .hadoopJarStep(step1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        steps.add(stepConfig1);

        //--------------------------------------------------------------------------------//

        HadoopJarStepConfig step2 = HadoopJarStepConfig.builder()
                .jar("s3://"+bucket+"/step2.jar")
                .mainClass("com.step2.Main")
                .args("s3://"+bucket+"/output1/", "s3://"+bucket+"/output2/")
                .build();

        StepConfig stepConfig2 = StepConfig.builder()
                .name("step 2")
                .hadoopJarStep(step2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        steps.add(stepConfig2);

        //--------------------------------------------------------------------------------//

        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar("s3://"+bucket+"/step3.jar")
                .mainClass("com.step3.Main")
                .args("s3://"+bucket+"/output2/", "s3://"+bucket+"/output3/")
                .build();

        StepConfig stepConfig3 = StepConfig.builder()
                .name("step 3")
                .hadoopJarStep(step3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        steps.add(stepConfig3);

        //--------------------------------------------------------------------------------//

        HadoopJarStepConfig step4 = HadoopJarStepConfig.builder()
                .jar("s3://"+bucket+"/step4.jar")
                .mainClass("com.step4.Main")
                .args("s3://"+bucket+"/output3/", "s3://"+bucket+"/output4/")
                .build();

        StepConfig stepConfig4 = StepConfig.builder()
                .name("step 4")
                .hadoopJarStep(step4)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        steps.add(stepConfig4);

        //--------------------------------------------------------------------------------//

        HadoopJarStepConfig step5 = HadoopJarStepConfig.builder()
                .jar("s3://"+bucket+"/step5.jar")
                .mainClass("com.step5.Main")
                .args("s3://"+bucket+"/output4/", "s3://"+bucket+"/output5/")
                .build();

        StepConfig stepConfig5 = StepConfig.builder()
                .name("step 5")
                .hadoopJarStep(step5)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        steps.add(stepConfig5);

        //--------------------------------------------------------------------------------//

        HadoopJarStepConfig step6 = HadoopJarStepConfig.builder()
                .jar("s3://"+bucket+"/step6.jar")
                .mainClass("com.step6.Main")
                .args("s3://"+bucket+"/output5/", "s3://"+bucket+"/output6/", String.valueOf(minPmi), String.valueOf(relMinPmi))
                .build();

        StepConfig stepConfig6 = StepConfig.builder()
                .name("step 6")
                .hadoopJarStep(step6)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        steps.add(stepConfig6);

        //--------------------------------------------------------------------------------//

        HadoopJarStepConfig step7 = HadoopJarStepConfig.builder()
        .jar("s3://"+bucket+"/step7.jar")
        .mainClass("com.step7.Main")
        .args("s3://"+bucket+"/output6/", "s3://"+bucket+"/output7/")
        .build();

        StepConfig stepConfig7 = StepConfig.builder()
                .name("step 7")
                .hadoopJarStep(step7)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        steps.add(stepConfig7);

        //--------------------------------------------------------------------------------//

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(9)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("3.3.1")
                .ec2KeyName("vockey")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .build();

        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("job")
                .releaseLabel("emr-6.4.0")
                .instances(instances)
                .steps(steps)
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .logUri("s3://"+bucket+"/logs/")
                .build();

        RunJobFlowResponse runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.jobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
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
}



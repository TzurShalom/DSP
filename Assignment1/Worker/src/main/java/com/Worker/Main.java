package com.Worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class Main
{
    static sentimentAnalysisHandler sentimentAnalysisHandler = new sentimentAnalysisHandler();
    static namedEntityRecognitionHandler namedEntityRecognitionHandler = new namedEntityRecognitionHandler();
    final static AWS aws = AWS.getInstance();
    final static String bucket = "bucket-sarcasm-analysis-eng";
    final static String queueManagerToWorkers = "manager-workers";
    final static String queueWorkersToManager = "workers-manager";
    final static String inputFile = "/usr/bin/workerPath/input.txt";
    final static String outputFile = "/usr/bin/workerPath/output.txt";
    static String text;
    static int sentiment;
    static List<String> entities;
    static Boolean crash = false;

    public static void main(String[] args)
    {
        char parse = '1';
        char perform = '2';
        char union = '3';
        char stop = '4';
        Message message = null;
        String inputKey = "null";
        String outputKey = "null";
        char task = '0';
        int n = 0;

        Thread thread = new Thread(() -> { CheckingTheManagerStatus(); });
        thread.start();

        while (true)
        {
            if (crash) { break; }

            if (!((message = aws.receiveMessage(queueManagerToWorkers)) == null)) //Worker gets a message from an SQS queue.
            {
                System.out.println("/--------------------/");
                System.out.println("Message received : " + message.body());

                task = message.body().charAt(0);

                if (task == parse)
                {
                    for (int i = 2; i < message.body().length(); i++)
                    {
                        if (message.body().charAt(i) == '#')
                        {
                            inputKey = message.body().substring(2, i);
                            n = Integer.parseInt(message.body().substring(i + 1));
                            break;
                        }
                    }

                    System.out.println("Action : Parse");
                    parse(inputKey,n);
                }
                else if (task == perform)
                {
                    for (int i = 2; i < message.body().length(); i++)
                    {
                        if (message.body().charAt(i) == '#')
                        {
                            inputKey = message.body().substring(2, i);
                            outputKey = message.body().substring(i + 1);
                            break;
                        }
                    }

                    System.out.println("Action : Perform");
                    perform(inputKey,outputKey);
                }
                else if (task == union)
                {
                    for (int i = 2; i < message.body().length(); i++)
                    {
                        if (message.body().charAt(i) == '#')
                        {
                            inputKey = message.body().substring(2, i);
                            outputKey = message.body().substring(i + 1);
                            break;
                        }
                    }

                    System.out.println("Action : Union");
                    union(inputKey,outputKey);
                }
                else if (task == stop)
                {
                    System.out.println("Action : Stop");
                    aws.deleteMessage(queueManagerToWorkers,message);
                    break;
                }

                aws.deleteMessage(queueManagerToWorkers,message);
            }
        }
    }

    public static void parse(String inputKey, int n)
    {
        int numberOfFiles = 1;

        String inputName = "null";
        for (int i = inputKey.length() - 1; i > 0; i--)
        {
            if (inputKey.charAt(i) == '/')
            {
                inputName = inputKey.substring(i + 1);
                break;
            }
        }

        if (crash) { return; }
        aws.downloadObjectFromBucket(bucket,inputKey,inputFile);

        //------------------------------------------------------------//

        int localApplication = 0;
        for (int i = 0; i < inputKey.length(); i++)
        {
            if (inputKey.charAt(i) == '/')
            {
                localApplication = Integer.parseInt(inputKey.substring(0,i));
                break;
            }
        }

        int numberOfReviews = 0;

        ObjectMapper objectMapper = new ObjectMapper();

        try
        {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
            FileWriter fileWriter = new FileWriter(outputFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            String line;
            while ((line = bufferedReader.readLine()) != null)
            {
                JsonNode reviewsNode = objectMapper.readTree(line).get("reviews");

                for (JsonNode reviewNode : reviewsNode)
                {
                    bufferedWriter.write(reviewNode.toString());
                    bufferedWriter.newLine();

                    numberOfReviews++;

                    if (numberOfReviews == n)
                    {
                        bufferedWriter.close();

                        for (int i = 0; i < inputName.length(); i++)
                        {
                            if (inputName.charAt(i) == '.')
                            {
                                inputName = inputName.substring(0,i);
                                break;
                            }
                        }

                        String key = String.valueOf(localApplication) + "/inputFromWorkers/"
                                + inputName + "_" + String.valueOf(numberOfFiles) + ".txt";

                        if (crash) { return; }
                        aws.uploadObjectToBucket(bucket,key,outputFile);
                        String message = 1 + "#" + key;
                        aws.sendMessage(queueWorkersToManager,message);
                        System.out.println("Message sent : " + message);

                        fileWriter = new FileWriter(outputFile);
                        bufferedWriter = new BufferedWriter(fileWriter);

                        numberOfFiles++;
                        numberOfReviews = 0;
                    }
                }
            }
            bufferedWriter.close();

        }
        catch (IOException e) { e.printStackTrace(); }
    }

    public static void perform(String inputKey, String outputKey)
    {
        if (crash) { return; }
        aws.downloadObjectFromBucket(bucket,inputKey,inputFile);

        ObjectMapper objectMapper = new ObjectMapper();
        boolean sarcasm = false;

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile)))
        {
            FileWriter fileWriter = new FileWriter(outputFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            String line;
            String link;
            int rating;
            JsonNode review;
            Thread thread;

            while ((line = bufferedReader.readLine()) != null)
            {
                sarcasm = false;
                review = objectMapper.readTree(line);
                text = review.get("text").asText();
                link = review.get("link").asText();
                rating = review.get("rating").asInt();

                thread = new Thread(() -> { sentiment = sentimentAnalysisHandler.findSentiment(text); });
                thread.start();

                entities = namedEntityRecognitionHandler.getEntities(text);

                try { thread.join(); }
                catch (InterruptedException e) { e.printStackTrace(); }

                if (Math.abs(sentiment - rating) > 2) { sarcasm = true; }

                bufferedWriter.write(link + "#" + sarcasm + "#" + sentiment + "#" + Arrays.toString(entities.toArray()));
                bufferedWriter.newLine();
            }

            bufferedWriter.close();
            if (crash) { return; }
            aws.uploadObjectToBucket(bucket, outputKey, outputFile);
        }
        catch (IOException e) { e.printStackTrace(); }

        if (crash) { return; }
        String message = 2 + "#" + outputKey;
        aws.sendMessage(queueWorkersToManager, message);
        System.out.println("Message sent : " + message);
    }

    public static void union(String inputKey, String outputKey)
    {
        if (crash) { return; }
        List<S3Object> list = aws.listObjects(bucket,inputKey);
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile)))
        {
            for (S3Object object : list)
            {
                if (crash) { return; }
                aws.downloadObjectFromBucket(bucket,object.key(),inputFile);
                try (BufferedReader reader = new BufferedReader(new FileReader(inputFile)))
                {
                    String line;
                    while ((line = reader.readLine()) != null)
                    {
                        bufferedWriter.write(line);
                        bufferedWriter.newLine();
                    }
                }
                catch (IOException e) { e.printStackTrace(); }
            }

            bufferedWriter.close();
            if (crash) { return; }
            aws.uploadObjectToBucket(bucket, outputKey, outputFile);
        }
        catch (IOException e) { e.printStackTrace(); }

        if (crash) { return; }
        String message = 3 + "#" + outputKey;
        aws.sendMessage(queueWorkersToManager, message);
        System.out.println("Message sent : " + message);
    }

    public static void CheckingTheManagerStatus()
    {
        String instanceId = aws.getInstanceIdByTag("Manager");

        while (true)
        {
            try { Thread.sleep(1000*5); }
            catch (InterruptedException e) { e.printStackTrace(); }
            if (!aws.isInstanceActive(instanceId))
            {
                System.out.println("The manager's computer crashed");
                crash = true;
                break;
            }
        }
    }
}
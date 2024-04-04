package com.LocalApplication;

import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main
{
    final static AWS aws = AWS.getInstance();
    final static String bucket = "bucket-sarcasm-analysis-eng";
    final static String queueToLocalApplication = "localApplication-newUser";
    final static String queueToManager = "manager-newUser";
    final static String queueLocalApplicationToManager = "localApplication-manager";
    final static String queueManagerToLocalApplication = "manager-localApplication";
    final static String queueManagerToWorkers = "manager-workers";
    final static String queueWorkersToManager = "workers-manager";
    static Boolean end = false;
    static Boolean crash = false;

    public static void main(String[] args)
    {
        //----------------------------------------------------------------------//

        System.out.println("Downloads the input files");

        String path = "C:/Users/MASTER/OneDrive/Desktop/207107038_206157117_1/";

        String inFilePath0 = path + "Assignment1/LocalApplication/inputFiles/input1.txt";
        String inFilePath1 = path + "Assignment1/LocalApplication/inputFiles/input2.txt";
        String inFilePath2 = path + "Assignment1/LocalApplication/inputFiles/input3.txt";
        String inFilePath3 = path + "Assignment1/LocalApplication/inputFiles/input4.txt";
        String inFilePath4 = path + "Assignment1/LocalApplication/inputFiles/input5.txt";;

        List<String> inFilePath = new ArrayList<>();
        inFilePath.add(inFilePath0);
//        inFilePath.add(inFilePath1);
//        inFilePath.add(inFilePath2);
//        inFilePath.add(inFilePath3);
//        inFilePath.add(inFilePath4);

        String outputFilePath = path + "Assignment1/LocalApplication/outputFiles/output.txt";
        String summaryFilePath = path + "Assignment1/LocalApplication/outputFiles/summary.html";

        String managerJar = path + "Assignment1/Manager/target/Manager-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String workerJar = path + "Assignment1/Worker/target/Worker-1.0-SNAPSHOT-jar-with-dependencies.jar";

        int n = 40;
        boolean t = true;

        //----------------------------------------------------------------------//

        System.out.println("Generates buckets and queues");

        aws.createBucketIfNotExists(bucket);

        aws.createQueueIfNotExists(queueToManager);
        aws.createQueueIfNotExists(queueToLocalApplication);

        aws.createQueueIfNotExists(queueLocalApplicationToManager);
        aws.createQueueIfNotExists(queueManagerToLocalApplication);

        aws.createQueueIfNotExists(queueManagerToWorkers);
        aws.createQueueIfNotExists(queueWorkersToManager);

        //----------------------------------------------------------------------//

        System.out.println("Uploads the manager's and worker's jar files");

        aws.uploadObjectToBucket(bucket,"manager/manager.jar",managerJar);
        aws.uploadObjectToBucket(bucket,"worker/worker.jar",workerJar);

        //----------------------------------------------------------------------//

        System.out.println("Starting the manager");

        String instanceId;

        if (!aws.isInstanceWithTagExists("Manager"))
        {
            aws.createInstance("Manager");
        }
        else
        {
            instanceId = aws.getInstanceIdByTag("Manager");
            if (!aws.isInstanceActive(instanceId)) { aws.startInstance(instanceId); }
        }

        try { Thread.sleep(1000*60); }
        catch (InterruptedException e) { e.printStackTrace(); }

        Thread thread = new Thread(() -> { CheckingTheManagerStatus(); });
        thread.start();

        //----------------------------------------------------------------------//

        aws.sendMessage(queueToManager,"1");
        System.out.println("(Local Application --> Manager) Message sent : Sending a request to join!");

        Message message = null;
        int localApplication = 0;

        while (true)
        {
            if (crash) { break; }

            if (!((message = aws.receiveMessage(queueToLocalApplication)) == null))
            {
                System.out.println("(Manager --> Local Application) Message sent : The request to join has been accepted!");
                localApplication = Integer.parseInt(message.body());
                aws.deleteMessage(queueToLocalApplication,message);
                break;
            }
        }

        //----------------------------------------------------------------------//

        System.out.println("Uploads the files to S3 and sends the location of the files to manager");

        String inFileKey = localApplication + "/inputForWorkers/input";

        for (int i = 0; i < inFilePath.size(); i++)
        {
            if (crash) { break; }

            aws.uploadObjectToBucket(bucket,inFileKey + i + ".txt",inFilePath.get(i));
            aws.sendMessage(queueLocalApplicationToManager,inFileKey + i + ".txt" + "#" + n + "#" + t);
            System.out.println("(Local Application --> Manager) Message sent : " + inFileKey + i + ".txt" + "#" + n + "#" + t);
        }

        //----------------------------------------------------------------------//

        System.out.println("Waiting for the manager's output");

        String localApplicationNumber = "0";
        String outFileKey = "null";

        while (true)
        {
            if (crash) { break; }

            if (!((message = aws.receiveMessage(queueManagerToLocalApplication)) == null))
            {
                for (int i = 0; i < message.body().length(); i++)
                {
                    if (message.body().charAt(i) == '/')
                    {
                        localApplicationNumber = message.body().substring(0,i);
                        break;
                    }
                }

                if (localApplication == Integer.parseInt(localApplicationNumber))
                {
                    System.out.println("(Manager --> Local Application) Message sent : " + message.body());
                    outFileKey = message.body();
                    aws.deleteMessage(queueManagerToLocalApplication,message);
                    end = true;
                    break;
                }
            }
        }

        //----------------------------------------------------------------------//

        if (!crash)
        {
            System.out.println("Summarizes the received outputs");

            String summaryFileKey;
            summaryFileKey = localApplication + "/summary.html";

            aws.downloadObjectFromBucket(bucket,outFileKey,outputFilePath);

            try
            {
                List<String> lines = readLinesFromFile(outputFilePath);
                String htmlOutput = generateHTML(lines);

                BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFilePath));
                writer.write(htmlOutput);
                writer.close();
            }
            catch (IOException e) { e.printStackTrace(); }

            aws.uploadObjectToBucket(bucket,summaryFileKey,summaryFilePath);
            System.out.println("The summary is ready!");
        }
    }

    //----------------------------------------------------------------------//

    public static String generateHTML(List<String> lines)
    {
        StringBuilder html = new StringBuilder();
        html.append("<html><body>");

        for (String line : lines)
        {
            String[] parts = line.split("#");

            String link = parts[0];
            boolean sarcasm = Boolean.parseBoolean((parts[1]));
            int sentiment = Integer.parseInt(parts[2]);
            String[] namedEntities = parts[3].replaceAll("[\\[\\]]", "").split(",");

            String color = "black";
            switch (sentiment)
            {
                case 1: color = "red"; break;
                case 2: color = "purple"; break;
                case 3: color = "blue"; break;
                case 4: color = "yellow"; break;
                case 5: color = "green"; break;
            }

            html.append("<p style=color:").append(color).append(";>").append(link).append("<span>");
            html.append("</span>").append("#").append(Arrays.toString(namedEntities)).append("<span>");
            html.append("</span>").append("#").append(sarcasm).append("</p>");
        }

        html.append("</body></html>");
        return html.toString();
    }

    //----------------------------------------------------------------------//

    public static List<String> readLinesFromFile(String filePath) throws IOException
    {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                lines.add(line);
            }
        }
        return lines;
    }

    //----------------------------------------------------------------------//

    public static void CheckingTheManagerStatus()
    {
        String instanceId = aws.getInstanceIdByTag("Manager");

        while (!end)
        {
            try { Thread.sleep(1000*5); }
            catch (InterruptedException e) { e.printStackTrace(); }
            if (!aws.isInstanceActive(instanceId))
            {
                System.out.println("The manager's computer crashed");
                //ServicesInitialization();
                crash = true;
                break;
            }
        }
    }

    //----------------------------------------------------------------------//

    public static void ServicesInitialization()
    {
        aws.emptyBucket(bucket);

        aws.purgeQueue(queueToManager);
        aws.purgeQueue(queueToLocalApplication);
        aws.purgeQueue(queueLocalApplicationToManager);
        aws.purgeQueue(queueManagerToLocalApplication);
        aws.purgeQueue(queueManagerToWorkers);
        aws.purgeQueue(queueWorkersToManager);
    }
}

package com.Manager;

import software.amazon.awssdk.services.sqs.model.Message;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Main
{
    final static AWS aws = AWS.getInstance();
    final static String queueToLocalApplication = "localApplication-newUser";
    final static String queueToManager = "manager-newUser";
    final static String queueLocalApplicationToManager = "localApplication-manager";
    final static String queueManagerToLocalApplication = "manager-localApplication";
    final static String queueManagerToWorkers = "manager-workers";
    final static String queueWorkersToManager = "workers-manager";
    static int localApplicationNumber = 1;
    static int distribution = 2;
    static int N = 8;
    static Boolean terminate = false;
    static Boolean end = false;
    static Map<Integer, Integer> numberOfFiles;
    static Map<Integer, Integer> fileCounter;
    static Map<Integer, Integer> numberOfWorkers;


    public static void main(String[] args)
    {
        numberOfFiles = new HashMap<>();
        fileCounter = new HashMap<>();
        numberOfWorkers = new HashMap<>();

        Thread thread1 = new Thread(() -> {communicationNewUsers();});
        thread1.start();

        Thread thread2 = new Thread(() -> {communicationLocalApplicationManager();});
        thread2.start();

        communicationManagerWorkers();
    }

    //----------------------------------------------------------------------------------------------------//

    public static void communicationNewUsers()
    {
        Message messageFromLocalApplication = null;

        while (true)
        {
            if (terminate) { break; }

            if (!((messageFromLocalApplication = aws.receiveMessage(queueToManager)) == null))
            {
                numberOfFiles.put(localApplicationNumber,0);
                fileCounter.put(localApplicationNumber,0);
                numberOfWorkers.put(localApplicationNumber,0);

                System.out.println("(Manager --> Local Application) User number " + localApplicationNumber + " has been created");

                aws.sendMessage(queueToLocalApplication,String.valueOf(localApplicationNumber));
                localApplicationNumber++;

                aws.deleteMessage(queueToManager,messageFromLocalApplication);
            }
        }
    }

    //----------------------------------------------------------------------------------------------------//

    public static void communicationLocalApplicationManager()
    {
        Message messageFromLocalApplication = null;
        String key = "null";
        String message = "null";
        int localApplication = 0;
        int n = 0;

        while (true)
        {
            if (end) { break; }

            if (!((messageFromLocalApplication = aws.receiveMessage(queueLocalApplicationToManager)) == null))
            {
                System.out.println("(Local Application --> Manager) Message received : " + messageFromLocalApplication.body());

                for (int i = 0; i < messageFromLocalApplication.body().length(); i++)
                {
                    if (messageFromLocalApplication.body().charAt(i) == '#')
                    {
                        key = messageFromLocalApplication.body().substring(0,i);
                        message = messageFromLocalApplication.body().substring(i + 1);
                        break;
                    }
                }

                for (int i = 0; i < message.length(); i++)
                {
                    if (message.charAt(i) == '#')
                    {
                        n = Integer.parseInt(message.substring(0,i)) / distribution;
                        terminate = Boolean.valueOf(message.substring(i + 1));
                        break;
                    }
                }

                for (int i = 0; i < key.length(); i++)
                {
                    if (key.charAt(i) == '/')
                    {
                        localApplication = Integer.parseInt(key.substring(0,i));
                        break;
                    }
                }

                aws.sendMessage(queueManagerToWorkers,1 + "#" + key + "#" + n);
                System.out.println("(Manager --> Worker) Message sent : " + 1 + "#" + key + "#" + n);

                numberOfWorkers.put(localApplication,numberOfWorkers.get(localApplication) + 1);
                startWorkers(numberOfWorkers.get(localApplication));

                aws.deleteMessage(queueLocalApplicationToManager,messageFromLocalApplication);

                try { Thread.sleep(1000*5); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }
        }
    }

    //----------------------------------------------------------------------------------------------------//

    public static void communicationManagerWorkers()
    {
        Message messageFromWorker = null;
        String messageToWorker = "null";
        String outputKey = "null";
        String[] array;
        int i,j = 0;

        while (true)
        {
            if (!((messageFromWorker = aws.receiveMessage(queueWorkersToManager)) == null))
            {
                System.out.println("(Worker --> Manager) Message received : " + messageFromWorker.body());

                char task = messageFromWorker.body().charAt(0);

                if (task == '1')
                {
                    array = extractInformationFromMessage1(messageFromWorker.body());

                    outputKey = array[1] + "/outputFromWorkers/output" + array[2] + ".txt";
                    messageToWorker = 2 + "#" + array[0] + "#" + outputKey;

                    numberOfFiles.put(Integer.parseInt(array[1]), numberOfFiles.get(Integer.parseInt(array[1])) + 1);
                    numberOfWorkers.put(Integer.parseInt(array[1]),numberOfWorkers.get(Integer.parseInt(array[1])) + 1);

                    aws.sendMessage(queueManagerToWorkers,messageToWorker);
                    System.out.println("(Manager --> Worker) Message sent : " + messageToWorker);

                    startWorkers(numberOfWorkers.get(Integer.parseInt(array[1])) / distribution);
                    aws.deleteMessage(queueWorkersToManager,messageFromWorker);
                }
                else if (task == '2')
                {
                    array = extractInformationFromMessage2(messageFromWorker.body());

                    if (fileCounter.containsKey((Integer.parseInt(array[0]))))
                    {
                        fileCounter.put(Integer.parseInt(array[0]), fileCounter.get(Integer.parseInt(array[0])) + 1);

                        if (fileCounter.get(Integer.parseInt(array[0])).equals(numberOfFiles.get(Integer.parseInt(array[0]))))
                        {
                            messageToWorker = 3 + "#" + array[0] + "/outputFromWorkers" + "#" + array[0] + "/output.txt";
                            aws.sendMessage(queueManagerToWorkers,messageToWorker);
                            System.out.println("(Manager --> Worker) Message sent : " + messageToWorker);

                            numberOfFiles.remove(Integer.parseInt(array[0]));
                            fileCounter.remove(Integer.parseInt(array[0]));
                        }
                    }
                    aws.deleteMessage(queueWorkersToManager,messageFromWorker);
                }
                else if (task == '3')
                {
                    array = extractInformationFromMessage3(messageFromWorker.body());

                    i = numberOfWorkers.get(Integer.parseInt(array[1])) / distribution;
                    numberOfWorkers.remove(Integer.parseInt(array[1]));

                    if (numberOfFiles.isEmpty() & terminate)
                    {
                        terminateWorkers();
                        end = true;
                        aws.sendMessage(queueManagerToLocalApplication, array[0]);
                        System.out.println("(Manager --> Local Application) Message sent : The output is ready!");
                        aws.deleteMessage(queueWorkersToManager,messageFromWorker);
                        break;
                    }
                    else if (numberOfFiles.isEmpty())
                    {
                        stopWorkers(i);
                        System.out.println("A : Sends a stop order to " + i + "workers");

                        aws.sendMessage(queueManagerToLocalApplication, array[0]);
                        System.out.println("(Manager --> Local Application) Message sent : The output is ready!");
                        aws.deleteMessage(queueWorkersToManager,messageFromWorker);
                    }
                    else
                    {
                        j = Collections.max(numberOfWorkers.values());
                        if (i > j)
                        {
                            stopWorkers(i - j);
                            System.out.println("B : Sends a stop order to " + i + "workers");
                        }

                        aws.sendMessage(queueManagerToLocalApplication, array[0]);
                        System.out.println("(Manager --> Local Application) Message sent : The output is ready!");
                        aws.deleteMessage(queueWorkersToManager,messageFromWorker);
                    }
                }
            }
        }
    }

    //----------------------------------------------------------------------------------------------------//

    private static String[] extractInformationFromMessage1(String message)
    {
        String key = "null";
        String file = "null";
        String number = "null";
        String localApplication = "null";

        String[] array = new String[3];

        for (int i = 0; i < message.length(); i++)
        {
            if (message.charAt(i) == '#')
            {
                key = message.substring(i + 1);
                break;
            }
        }

        array[0] = key;

        for (int i = 0; i < key.length(); i++)
        {
            if (key.charAt(i) == '/')
            {
                localApplication = key.substring(0,i);
                break;
            }
        }

        array[1] = localApplication;

        for (int i = key.length() - 1; i > 0; i--)
        {
            if (key.charAt(i) == '/')
            {
                file = key.substring(i + 1);
                break;
            }
        }

        for (int i = 5; i < file.length(); i++)
        {
            if (file.charAt(i) == '.')
            {
                number = file.substring(5,i);
                break;
            }
        }

        array[2] = number;
        return array;
    }

    //----------------------------------------------------------------------------------------------------//

    private static String[] extractInformationFromMessage2(String message)
    {
        String key = "null";
        String file = "null";
        String number = "null";
        String localApplication = "null";

        String[] array = new String[2];

        for (int i = 0; i < message.length(); i++)
        {
            if (message.charAt(i) == '#')
            {
                key = message.substring(i + 1);
                break;
            }
        }

        for (int i = 0; i < key.length(); i++)
        {
            if (key.charAt(i) == '/')
            {
                localApplication = key.substring(0,i);
                break;
            }
        }

        array[0] = localApplication;

        for (int i = key.length() - 1; i > 0; i--)
        {
            if (key.charAt(i) == '/')
            {
                file = key.substring(i + 1);
                break;
            }
        }

        for (int i = 6; i < file.length(); i++)
        {
            if (file.charAt(i) == '_')
            {
                number = file.substring(6,i);
                break;
            }
        }

        array[1] = number;

        return array;
    }

    //----------------------------------------------------------------------------------------------------//

    private static String[] extractInformationFromMessage3(String message)
    {
        String key = "null";
        String localApplication = "null";

        String[] array = new String[2];

        for (int i = 0; i < message.length(); i++)
        {
            if (message.charAt(i) == '#')
            {
                key = message.substring(i + 1);
                break;
            }
        }

        array[0] = key;

        for (int i = 0; i < key.length(); i++)
        {
            if (key.charAt(i) == '/')
            {
                localApplication = key.substring(0,i);
                break;
            }
        }

        array[1] = localApplication;
        return array;
    }

    //----------------------------------------------------------------------------------------------------//

    private static void startWorkers(int numberOfWorkers)
    {
        String instanceId;

        for (int i = 1; (i <= numberOfWorkers) & (i <= N); i++)
        {
            if (!aws.isInstanceWithTagExists("Worker" + i))
            {
                aws.createInstance("Worker" + i);
                System.out.println("Worker " + i + " is created");
            }
            else
            {
                instanceId = aws.getInstanceIdByTag("Worker" + i);
                if (!aws.isInstanceActive(instanceId))
                {
                    aws.startInstance(instanceId);
                    System.out.println("Worker " + i + " started");
                }
            }
        }
    }

    //----------------------------------------------------------------------------------------------------//

    private static void stopWorkers(int numberOfWorkers)
    {
        for (int i = 1; i <= numberOfWorkers; i++)
        {
            aws.sendMessage(queueManagerToWorkers,"4");
        }
    }

    private static void terminateWorkers()
    {
        String instanceId;
        int numberOfWorkers = 10;

        for (int i = 1; i <= numberOfWorkers; i++)
        {
            if (aws.isInstanceWithTagExists("Worker" + i))
            {
                instanceId = aws.getInstanceIdByTag("Worker" + i);
                aws.terminateInstance(instanceId);
            }
        }
    }
}



----------

How to run our project:
	1. Download the zip file
	2. Open the localApplication program through IDE
	3. mvn clean + mvn package for manager's and worker's files (create jar files)
	3. Update the path variable according to the path where you saved the files
	4. Enter values for the environment variables
	5. Run the program

Explanation of how our program works:
	1. The Local Application sends the manager a request to connect
	2. The manager approves the request and gives the Local Application a unique id
	3. The Local Application receives the approval and uploads his input files to S3,
		and sends their keys (locations) to the manager
	4. The manager receives the keys and starts one worker per file
	5. The manager sends the workers the keys
	6. The workers parse the files and divide each file into 2n files
	7. The workers upload these files to S3 and send their keys to the manager
	8. The manager receives the message and sends the workers to work on these files
	9. The worker work on a file and when finished he uploads the processed file to S3
	10. The manager waits for all workers to finish their files (per client)
		and then sends a message to a worker with a request to merge the processed files
	11. The worker merges the files, uploads the merged file to S3 and updates the manager
	12. The manager sends a massege to the client that the output is ready (and its key)
	13. The Local Application creates html file from the given output file

Type of instance we used:
	Manager - ami-0c7217cdde317cfec, type-M4.large.
	Workwrs - ami-0c7217cdde317cfec, type-M4.large.
	(We need computers of this type to provide larger memory and the possibility of running multiple threads on the computer,
	also, the m4 series is more stable and reliable than the t2 series).

Time it took our program to finish working on the input files:
	On average - 7 minutes 
	(Including the upload and download times of the jar files)
	(It depends on the value of n - if there are very long reviews in the file then we will prefer a smaller n,
	and if there are short reviews in the file then we will use a larger n)

What was the n we used:
	n = 40,60,80.

----------

Mandatory Requirements:

▪ Did you think for more than 2 minutes about security?
	Yes, we use the command "iamInstanceProfile" that connects AWS, so we don't need to send the credentials in plain text.

▪ Did you think about scalability?
	Yes, our program is supposed to be scalable, thanks to the threads in the manager and workers that enable dynamic resource allocation.

▪ What about persistence?
	* We used the visibility mechanism to have a "backup" for the messages in the queues,
		so in case a worker fails we can make sure his task is done by a different worker.

▪ Threads in your application, when is it a good idea? When is it bad?
	Using threads is a good idea in the Manager for communicating in parallel with the Local App and the Workers.
		Also when we want to divide tasks and perform them in parallel.
	Using threads is a bad idea in case when threads are created per Local App/ Worker, because it harms the scalability.

▪ Did you run more than one client at the same time?
	Yes, It worked properly and finished as expected.

▪ Do you understand how the system works?
	Yes. At first it was difficult to understand how the system works,
		but as we progressed in implementing the system things became clearer. :)

▪ Did you manage the termination process?
	Yes, we created a boolean variable in the manager that determines whether to terminate after the workers have finished
		their work for the previous clients, and we check it's value in all the threads in the manager.

▪ Did you take in mind the system limitations that we are using?
	Yes, we are aware of the limitation of 9 EC2 instances, so when we create the workers we make sure not to create more
		then 8 workers (8 workers + 1 manager = 9 EC2 instances).

▪ Are all your workers working hard? Or some are slacking? Why?
	This may depend on the size of the reviews within the file.
		But we tried to distribute the load by splitting the reviews into more files, so that a worker who finishes his file
		will immediately move to the next file if necessary, while a worker who received a file with very long reviews will
		continue to work on it.

▪ Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks?
	We think the manager is not doing more then he's supposed to. We implemented each part as required in the Instructions file.

▪ Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?
	Yes, we understand what distributed mean. We can say that the Local Application is "waiting", meaning it is constantly
		listening to the manager.

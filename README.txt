Kseniia Martin 341223055 Rozi Rabaev 319589412

Instructions of how to run our project: 
 In the command line you should type : java -jar assignment1.jar (input1 .... inputn) (output1... outputn) n terminate(optional)  for running local app.
if you would like to run a single worker you should type :  java -jar Worker.jar
for access the aws services you should have the credentials (keys) at a file named "credentials" at folder ".aws" at your user's folder. 

explanation of how the program works: 
Local App:
First, the local app creates ec2 instance of manager if doesnt exists. Than for each input file uploads it to s3 bucket with the required n anfd the file name,  under a folder name specified (if doesnt exist - creat it).
Then sends  relevant message to manager via queue that the file has uploaded, after that the local app waits for response from the manager with summary file. 
When recieved a message from the manager with the location of the summary file, downloads the file and create html file as output. 

Manager:
Setting up the workers handler thread for treating the workers ( creating them according to the specified n).
Recieves message from local app with the location of the input files, then creates runnable task from the message and send it to execution by thread pool we've created. 
When it recieves delete message, sends terminate message to task manager queue, then waits for threads to finish their job and after they finish it delete the bucket. 


Task Manager: 
Recieves message from task manager with a task to execute, with required n and file name of the input. Downloads the input file and analyzed the number of reviews in it. 
Then sends a message to workers handler with n and number of revies, for it to create the appropriate number of workers. Waits for workers handler to finish initialize the workers.
Then for each review send the current thead id with it to jobs queue and waits for output for each review, then creates summary and sends it to s3. 
Sends message to local app with the location of the relavent summary file. 


Workers Handler: 
Gets messages from task manager with number of reviews and n, and creates the appropriate number of worker instances. 
If recieved terminate message, terminate those instances that it created.

Worker:
Creates Message Visibilty Handler for taking care of visibility time out ( in a way that increases the visibility timeout up untill the worker has'nt finished the job). 
Recieves a message from task manager with the review and tread id. Preformes the algorithm for detecting sarcasm and when finish, delete the message from the queue, 
and sendes the tread id and review's analysis to task manager. 


Type of instances: Micro for manager and Medium for workers. ( to handle nlp libreries)

The n that we've selected n=50. 
The ammount of time required for the program to run with n=50 and 5 input files is 18 minutes.

Requirements for readme file:
1. security - we've created role with the requiered permissions and add it to the ec2 instances. 
2. scalability - we added thread pool so the manager could take care of more than one local app at the same time. 
3. persistence - for example if a node dies (worker node) the message is still at the queue because its get deleted inly after it handeled, 
so other worker will take this message (of the dead node) and will take of it. If a node stalls for a while, we've implemented visibillity time out handler that increas the visibility time out of the worker.
we have allocated an amount of time for each worker, so it means a worker can stalls for a while but not for a long time. 
4. Threads in our application  - in the manager we have thread pool (for scalability as mentioned above), workers handler (that take care of the number of the workers)- for all threads from tread pool and
visibility handler is also a thread. 
5. We run more than one client at the same time and they work properly, and finish properly, and the results are correct.
6.  Termination process - once the local app recieved terminate argument it sends it to the manager wich waits up untill all threads finish their jobs and then it closes 
and terminates them all and deletes all files and buckets from s3.
7. System limitations - we chose an N wich servers us the best in a terms of money and in a way wich wont exceed the amount of instances we can use at same time at our student account. 
8. Each part of our system has properly defined tasks as mentioned above. 
9.A distributed system is a system with multiple components  on different machines, in a similar way our program have multiple components on different instances that have defined tasks.

*A system structure is at the zip file. 


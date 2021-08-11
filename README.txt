A real-world application to distributively process a list of amazon
reviews, perform sentiment analysis and named-entity recognition, and display the result on a web
page. The application is composed of a local application, and non-local instances running on Amazon
cloud.
The application gets input text files containing lists of reviews (JSON format).
Then, instances are launched in AWS (workers & a manager) to apply sentiment analysis on the
reviews and detect whether it is sarcasm or not. The results are displayed on a webpage.

Instructions of how to run our project: 
 In the command line you should type : java -jar assignment1.jar (input1 .... inputn) (output1... outputn) n terminate(optional)  for running local app.
if you would like to run a single worker you should type :  java -jar Worker.jar
for access the aws services you should have the credentials (keys) at a file named "credentials" at folder ".aws" at your user's folder. 

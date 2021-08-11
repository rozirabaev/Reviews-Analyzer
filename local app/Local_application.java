import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.Base64;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import java.util.LinkedList;
import java.util.List;

import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

public class Local_application {
    private static S3Client s3;
    private static SqsClient sqs;
    private static Ec2Client ec2;

    private static Boolean is_exist() {
        Boolean done = false;
        String nextToken = null;
        Boolean exist = false;
        try {

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                       /* System.out.printf(
                                "Found Reservation with id %s, " +
                                        "AMI %s, " +
                                        "type %s, " +

                                        "state %s " +
                                        "and monitoring state %s",
                                instance.instanceId(),
                                instance.imageId(),
                                instance.instanceType(),
                                instance.state().name(),
                                instance.monitoring().state());
                        System.out.println("");*/
                        if (instance.tags().size() != 0) {
                            //System.out.println(instance.tags().get(0).key());
                            if (instance.tags().get(0).key().equals("Manager") &&
                                    (instance.state().nameAsString().equals("running")))
                                exist = true;
                        }

                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return exist;
    }

    private static void create_ec2(String name, String amiId, File manager_code) throws IOException {

        ec2 = Ec2Client.create();
        if (!is_exist()) {


            send_file("bguass01jars","Manager",new File("Manager.jar"));
            String user_data = "#!/bin/bash\n" + "aws s3 cp s3://bguass01jars/assignment1/Manager ./Manager.jar"+"\n"+
                    "java -jar Manager.jar\n";

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MEDIUM)
                    .imageId(amiId)
                    .maxCount(1)
                    .minCount(1)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::628940966620:instance-profile/ec2_role").build())
                    .userData(Base64.getEncoder().encodeToString(user_data.getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

            String instanceId = response.instances().get(0).instanceId();

            Tag tag = Tag.builder()
                    .key("Manager")
                    .value(name)
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            try {
                ec2.createTags(tagRequest);
                System.out.printf(
                        "Successfully started manager EC2 instance %s based on AMI %s\n",
                        instanceId, amiId);

            } catch (Ec2Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }

        }

    }

    private static void deleteBucket(String bucket) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        //s3.deleteBucket(deleteBucketRequest);
    }

    private static void create_s3(String bucket_name) {
        Region region = Region.US_EAST_1;
        s3 = S3Client.builder().region(region).build();

        createBucket(bucket_name, region);

    }

    private static void createBucket(String bucket, Region region) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .build());



    }

    private static void send_file(String bucket, String key, File input_file) throws IOException {
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key("assignment1/" + key)
                        .build(),
                RequestBody.fromBytes((Files.readAllBytes(input_file.toPath()))));

    }

    private static void create_queue(String queue_name) {
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();


        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue_name)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            //throw e;

        }

    }

    private static void send_message(String message, String queue_name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);

    }

    private static Boolean is_done(String queue_name, String key) {
        // receive messages from the queue
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        for (Message m : messages) {
            if (m.body().equals(key)) {
                delete_message(m, queue_name);
                return true;
            }
        }
        return false;
    }

    private static void delete_folder(String bucket, String folder) {
        ListObjectsV2Request listObjectsReqManual = ListObjectsV2Request.builder()
                .bucket(bucket)
                .maxKeys(1)
                .prefix(folder)
                .build();
        ListObjectsV2Response listObjResponse = s3.listObjectsV2(listObjectsReqManual);
        List<String> keysList = new LinkedList<String>();//[ listObjResponse.contents().size() ];
        listObjResponse.contents().stream()
                .forEach(content -> keysList.add(content.key()));
        for (String key : keysList) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();
            s3.deleteObject(deleteObjectRequest);
        }

    }

    public static String get_url(String queue_name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
    }

    private static void delete_message(Message message, String queue_name) {
        String queueUrl = get_url(queue_name);

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    private static String get_string(String[] str_arr) {
        String ret = "";
        for (String s : str_arr) {
            ret += s + ", ";
        }
        if(str_arr.length>0)
            ret.substring(0, ret.length() - 1);
        return ret;
    }

    private static void create_html(String out_name,String file_name) throws IOException {
        Gson gson = new Gson();
        List<Review_analysis> reviews = new LinkedList<Review_analysis>();
        int c = 0;
        try {
            JsonReader reader = new JsonReader(new FileReader(file_name));
            reader.setLenient(true);
            while (reader.peek() != JsonToken.END_DOCUMENT) {
                reviews.add(gson.fromJson(reader, Review_analysis.class));
                //System.out.println(reviews.reviews[0].id);
                c++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("local app: "+c+" reviews from reader");
        StringBuilder body = new StringBuilder();
        System.out.println("Local app: got "+reviews.size()+" reviews from manager");
        int i=0;
        for (Review_analysis rev : reviews) {
            String color;
            switch (rev.sentiment) {
                case 1:
                    color = "style=\"color:#9B1D02\";";
                    break;
                case 2:
                    color = "style=\"color:#FB2E03\";";
                    break;
                case 3:
                    color = "style=\"color:#000000\";";
                    break;
                case 4:
                    color = "style=\"color:#C0F9A0\";";
                    break;
                case 5: //dark green
                    color = "style=\"color:#4E881C \";";
                    break;
                default:
                    color = "";
                    break;
            }
            String link = "<a href=" + rev.link + " " + color + ">" + "Link" + "</a>";
           //String line= "<p>" +i + "</p>";
            String line = "<p> " + link + " Entities: " + get_string(rev.entities) + " Sarcasm: " + rev.sarcasm + "</p>";
            body.append(line);
            i++;
        }
        String html_str = "<html> <head>" +
                " <title>Reviews - sarcasm analysis</title>" +
                " </head>" + "<body>" + body + "</body>" +
                "</html>";
        FileWriter myWriter = new FileWriter(out_name+".html");
        myWriter.write(html_str);
        myWriter.close();

    }

    public static void main(String[] args) throws IOException {
        System.setProperty("aws.region", "us-east-1");
        File manager_code = new File("Manager.c");

        String name = "ec2";
        String amiId = "ami-0594bffef857a2f3e";
        String bucket_name = "bguass01";

        create_s3(bucket_name);
        create_s3("bguass01jars");

        create_ec2(name, amiId, manager_code);

        create_queue("Local_app");
        create_queue("Manager");
        send_file("bguass01jars","Worker",new File("Worker.jar"));

        boolean terminate = false;        int last = args.length;
        if (args[args.length - 1].equals("terminate")) {
            terminate = true;
            last--;
        }

        String key_n = args[last - 1];

        for (int i = 0; i < (last - 1)/2; i++) {
            String key = args[i];
            File input_file = new File(key + ".txt");
            System.out.println("Local app: sending file " + key + " to s3 and message to the manager");
            send_file(bucket_name, key, input_file);
            send_message(key_n + " " + key, "Local_app");


            boolean done = false;
            System.out.println("Local app: waiting...");
            while (!done) {
                done = is_done("Manager", key);
            }
            System.out.println("Local app: getting output file for " + key);
            s3.getObject(GetObjectRequest.builder().bucket(bucket_name).key("assignment1/" + key).build(),
                    ResponseTransformer.toFile(Paths.get("output_" + key)));
            create_html(args[i+(last - 1)/2],"output_" + key);

        }

        if (terminate)
            send_message("terminate", "Local_app");


        //TODO: we can create s3,sqs and ec2 operations classes to prevent double code(send message, send file, etc)


    }
}




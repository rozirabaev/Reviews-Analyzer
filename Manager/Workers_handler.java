import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;

public class Workers_handler extends Thread {
    static private int size;
    static private SqsClient sqs_handler;
    static private SqsClient sqs_task;

    static private Ec2Client ec2;

    private static SqsClient create_queue(String queue_name) {
        SqsClient sqs_ = SqsClient.builder().region(Region.US_EAST_1).build();


        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue_name)
                    .build();
            CreateQueueResponse create_result = sqs_.createQueue(request);
        } catch (QueueNameExistsException e) {
            //throw e;

        }
        return sqs_;

    }

    public static String get_url(String queue_name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        switch (queue_name) {
            case "task_manager":
                return sqs_task.getQueueUrl(getQueueRequest).queueUrl();
            case "Workers_handler":
                return sqs_handler.getQueueUrl(getQueueRequest).queueUrl();

        }
        return "-1";
    }

    public static List<Message> get_messages(String url) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(url)
                .build();
        List<Message> messages = sqs_task.receiveMessage(receiveRequest).messages();
        return messages;
    }

    private static void send_message(String message, String queue_name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs_handler.getQueueUrl(getQueueRequest).queueUrl();

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs_handler.sendMessage(send_msg_request);

    }
    private static void send_file(String bucket, String key, File input_file) throws IOException {
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();

        s3.putObject(PutObjectRequest.builder().bucket(bucket).key("assignment1/" + key)
                        .build(),
                RequestBody.fromBytes((Files.readAllBytes(input_file.toPath()))));

    }

    private static void create_workers(String name, String amiId, int n) throws IOException {

        String user_data = "#!/bin/bash\n" + "aws s3 cp s3://bguass01jars/assignment1/Worker ./Worker.jar"+"\n"+
                "java -jar Worker.jar\n";
        for(int i=0;i<n;i++) {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MEDIUM)
                    .imageId(amiId)
                    .maxCount(1)
                    .minCount(1)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::628940966620:instance-profile/ec2_role").build())
                    .userData(Base64.getEncoder().encodeToString(user_data.getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
           // for (int i = 0; i < response.instances().size(); i++) {
                String instanceId = response.instances().get(0).instanceId();

                Tag tag = Tag.builder()
                        .key("Worker")
                        .value(name)
                        .build();

                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                        .resources(instanceId)
                        .tags(tag)
                        .build();

                try {
                    ec2.createTags(tagRequest);


                } catch (Ec2Exception e) {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }
            }

        System.out.printf(
                "Successfully started %d worker EC2 instances  based on AMI %s\n", n,
                amiId);

    }

    private static void delete_message(Message message, String queue_name) {
        String queueUrl = get_url(queue_name);

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs_task.deleteMessage(deleteRequest);
    }

    private static void remove_workers(){
        String nextToken = null;

        try {

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        if (instance.tags().size() != 0) {
                            //  System.out.println(instance.tags().get(0).key());
                            if (instance.tags().get(0).key().equals("Worker") &&
                                    (instance.state().nameAsString().equals("running"))) {
                                ec2.terminateInstances( TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());


                            }

                        }

                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void run() {
        String name = "ec2";
        String amiId = "ami-0594bffef857a2f3e";
        ec2 = Ec2Client.create();
        String queue_name = "task_manager";
        sqs_handler = create_queue(queue_name);
        sqs_task = create_queue("Workers_handler");
        System.out.println("Workers handler: starting");
        boolean terminate = false;
        while (!terminate) {
            String queueUrl = get_url(queue_name);
            List<Message> messages = get_messages(queueUrl);
            for (Message m : messages) {

                if (m.body().equals("terminate")) {
                    remove_workers();
                    terminate=true;
                    delete_message(m, queue_name);
                    break;
                }
                String[] n_key = m.body().split(" ", 2);
                int n = Integer.parseInt(n_key[0]);
                int num_rev = Integer.parseInt(n_key[1]);
                float r = (float) num_rev / n;
                System.out.println("Workers handler:  n = " + n + " and " + num_rev + " reviews");
                if (Math.ceil(r) > size) {
                    int new_workers = (int) Math.ceil(r) - size;
                    try {
                        create_workers(name, amiId, new_workers);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    size = n;
                }
                delete_message(m, queue_name);
                send_message(m.body(), "Workers_handler");
            }
        }
    }
}

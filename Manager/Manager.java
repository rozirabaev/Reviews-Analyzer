import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager   {
    static  private SqsClient sqs;
    private static void create_queue(String queue_name){
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
    public  static List<Message> get_messages(String url){
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(url)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return messages;
    }
    public static  String get_url(String queue_name){
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return  queueUrl;
    }
    private static void delete_message(Message m, String queue_name) {
        String queueUrl = get_url(queue_name);

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(m.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
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
    private static void delete_folder(String bucket, String folder) {
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();

        ListObjectsV2Request listObjectsReqManual = ListObjectsV2Request.builder()
                .bucket(bucket)
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
    private static void send_file(String bucket, String key, String out) throws IOException {
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();

        s3.putObject(PutObjectRequest.builder().bucket(bucket).key("assignment1/" + key)
                        .build(),
                RequestBody.fromBytes(out.getBytes()));

    }

    public static void main(String[] args) throws InterruptedException, IOException {
        System.setProperty("aws.region","us-east-1");

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
        Workers_handler handler = new Workers_handler();
        handler.start();
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        create_queue("Local_app");
        create_queue("jobs");
        create_queue("output");
        String queue_name = "Local_app";
        Boolean terminate = false;
        while (!terminate){
            //  System.out.println("Manager: getting messages");
            String queueUrl = get_url(queue_name);
            List<Message> messages = get_messages(queueUrl);
            for(Message m: messages){
                task_manager task = new task_manager(m);
                if (m.body().equals("terminate")){
                    delete_message(m, queue_name);
                    terminate = true;
                }
                else {
                    executor.execute(task);
                    delete_message(m,queue_name);
                }
            }
        }

        executor.shutdown();
       // executor.awaitTermination(2, TimeUnit.SECONDS);
        send_message("terminate","task_manager");
        handler.join();
        delete_folder("bguass01", "assignment1");
        delete_folder("bguass01jars", "assignment1");

    }

}

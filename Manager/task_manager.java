import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class task_manager implements Runnable {
    private  Message message;
    private   S3Client s3;
    private  List<Reviews> reviews;
    private  String output;
    private   SqsClient sqs;

    public  String get_url(String queue_name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
    }

    private void delete_message(Message m, String queue_name) {
        String queueUrl = get_url(queue_name);

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(m.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    private void read_json(String file_name) {
        Gson gson = new Gson();
        reviews = new LinkedList<Reviews>();

        try {
            JsonReader reader = new JsonReader(new FileReader(file_name));
            reader.setLenient(true);
            while (reader.peek() != JsonToken.END_DOCUMENT) {
                reviews.add(gson.fromJson(reader, Reviews.class));
                //System.out.println(reviews.reviews[0].id);

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private  void send_message(String message, String queue_name) {
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

    private  void send_file(String bucket, String key, String out) throws IOException {
        FileWriter myWriter = new FileWriter("output"+key);
        myWriter.write(out);
        myWriter.close();
        File fout = new File("output"+key);

        s3.putObject(PutObjectRequest.builder().bucket(bucket).key("assignment1/" + key)
                        .build(),
                RequestBody.fromBytes((Files.readAllBytes(fout.toPath()))));


    }

    public  List<Message> get_messages(String url) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(url)
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return messages;
    }

    public  String get_str(Review rev){
        Gson gson = new Gson();
        //   System.out.println(gson.toJson(rev));
        return gson.toJson(rev);
    }

    public void send_reviews(){
        int count = 0;
        for(Reviews rev: reviews){
            for(Review review: rev.reviews){
               // if(count==10)
                 //   break;;
                String r = get_str(review);
                send_message(Thread.currentThread().getId()+" "+r,"jobs");
                count++;
            }
           // if(count==10)
             //   break;;
        }

        //System.out.println("sending "+count);

    }

    public void get_output(int reviews_num){
        int count = 0;
       // reviews_num = 10;
        output = "";
        while(!(count==reviews_num)) {
            String queueUrl = get_url("output");
            List<Message> messages = get_messages(queueUrl);
            for (Message m : messages) {
                if(count==reviews_num)
                    break;
                String[] id_output = m.body().split(" ",2);
                int id = Integer.parseInt(id_output[0]);
                if (id == (int)Thread.currentThread().getId()) {
                    output=output+"\n"+id_output[1];
                    delete_message(m, "output");
                    count++;
                }

            }
        }
        System.out.println("Thread "+Thread.currentThread().getId()+" count = "+count);

    }

    public  int num_reviews(){
        int reviews_num = 0;
        for (Reviews r : reviews) {
            reviews_num += r.reviews.length;
        }
        return  reviews_num;
    }

    public  void waiting_for_ec2setup(String s){
        Boolean done = false;
        while (!done) {
            String queueUrl = get_url("Workers_handler");
            List<Message> messages = get_messages(queueUrl);
            for (Message m : messages) {
                if (m.body().equals(s)) {
                    done = true;
                    delete_message(m, "Workers_handler");
                }
            }
        }

    }

    public task_manager(Message m) {
        message = m;
        Region region = Region.US_EAST_1;
        s3 = S3Client.builder().region(region).build();

    }

    private void delete_file(String file_name) {
        File f = new File(file_name);
        f.delete();
    }

    public void run() {
        String bucket_name = "bguass01";
        String[] n_key = message.body().split(" ");
        int n = Integer.parseInt(n_key[0]);
        String key = n_key[1];
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        System.out.println("Task manager received message - "+ message.body()+" from local app");
        String bucket = "bguass01";
        String file_name = "out_" + key;
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key("assignment1/" + key).build(),
                ResponseTransformer.toFile(Paths.get(file_name)));
        read_json(file_name);
        //delete_file(file_name);//removes file that we don't  need now
        int reviews_num = num_reviews();

        send_message(n+" "+reviews_num, "task_manager");

        waiting_for_ec2setup(n+" "+reviews_num);

        send_reviews();
        System.out.println("Thread "+ Thread.currentThread().getId()+" send "+reviews_num+" reviews");

        get_output(reviews_num);

        System.out.println("Task manager: sending message to local app and exits");
        try {
            System.out.println("Thread "+Thread.currentThread().getId()+ ": sending file - "+ key);
            send_file(bucket_name,key,output);
        } catch (IOException e) {
            e.printStackTrace();
        }
        send_message(key, "Manager");
        delete_message(message,"Local_app");



    }


}
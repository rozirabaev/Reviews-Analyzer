import java.io.IOException;

import com.google.gson.Gson;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class Worker {
    static sentimentAnalysisHandler sentimentAnalysisHandler;
    static namedEntityRecognitionHandler namedEntityRecognitionHandler;
    public static Review review;
    private static SqsClient sqs_in;
    private static SqsClient sqs_out;
    private static Review_analysis review_out;

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
        String queueUrl = sqs_in.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
    }

    public static List<Message> get_messages(String url) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(url)
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = sqs_in.receiveMessage(receiveRequest).messages();
        return messages;
    }

    public static void get_review(String json) {
        Gson gson = new Gson();
       // System.out.println(json);
        review = gson.fromJson(json, Review.class);
    }

    public static String get_output() {
        Gson gson = new Gson();
        return gson.toJson(review_out);
    }

    public static String is_sarcasm(int sentiment) {
        if ((review.rating < 3 && sentiment > 3) || (review.rating > 3 && sentiment < 3))
            return "true";
        else
            return "false";
    }

    private static void delete_message(Message message, String queue_name) {
        String queueUrl = get_url(queue_name);

        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs_in.deleteMessage(deleteRequest);
    }

    private static void send_message(String message, String queue_name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queue_name)
                .build();
        String queueUrl = sqs_out.getQueueUrl(getQueueRequest).queueUrl();

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs_out.sendMessage(send_msg_request);

    }


    public static void main(String[] args) throws IOException {

        sentimentAnalysisHandler = new sentimentAnalysisHandler();
        namedEntityRecognitionHandler = new namedEntityRecognitionHandler();
        System.setProperty("aws.region", "us-east-1");

        sqs_in = create_queue("jobs");
        sqs_out = create_queue("output");
        while (true) {
            String queueUrl = get_url("jobs");


            List<Message> messages = get_messages(queueUrl);

            if(messages.size()!=0) {
                System.out.println("Worker got message");
                Message m = messages.get(0);
                message_visibility_handler visibility_handler = new message_visibility_handler(m, queueUrl, sqs_in);

                visibility_handler.start();
                if (m.body().equals("terminate"))
                    break;

                String[] id_obj = m.body().split(" ", 2);
                get_review(id_obj[1]);
                review_out = new Review_analysis();
                int sentiment = sentimentAnalysisHandler.findSentiment(m.body());
                System.out.println(sentiment);
                List<String> e = namedEntityRecognitionHandler.findEntities(m.body());

                String[] entities = new String[e.size()];
                e.toArray(entities);
                review_out.link = review.link;
                review_out.sentiment = sentiment;
                review_out.entities = entities;
                review_out.sarcasm = is_sarcasm(sentiment);
                review_out.title = review.title;
                String message_out = get_output();
                delete_message(m, "jobs");
                send_message(id_obj[0] + " " + message_out, "output");
                visibility_handler.stop_me();

            }
        }
    }
}

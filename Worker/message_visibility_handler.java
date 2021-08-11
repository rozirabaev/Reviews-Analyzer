import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;

public class message_visibility_handler extends Thread {
    public Message message;
    public String queueUrl;
    public   SqsClient sqs_in;
    public boolean exit;
    public int time;

    public message_visibility_handler(Message m, String url, SqsClient sqs ){
        this.message = m;
        this.queueUrl = url;
        this.sqs_in = sqs;
        this.exit = false;
        this.time=0;
    }

    public void stop_me(){
        this.exit = true;
    }
    public void run(){
        while(!exit) {
            if(time<2) {
                ChangeMessageVisibilityRequest rec = ChangeMessageVisibilityRequest.builder()
                        .queueUrl(queueUrl)
                        .visibilityTimeout(120)
                        .receiptHandle(message.receiptHandle())
                        .build();
                ChangeMessageVisibilityResponse res = sqs_in.changeMessageVisibility(rec);
            }
            else
                break;

            try {
                sleep(60000);
                time++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

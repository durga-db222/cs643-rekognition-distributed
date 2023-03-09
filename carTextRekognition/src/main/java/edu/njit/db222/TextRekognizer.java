package edu.njit.db222;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import edu.njit.db222.Utils.Constants;
import edu.njit.db222.Utils.ImageRecognitionMessage;
import edu.njit.db222.Utils.QueueMessage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectTextRequest;
import software.amazon.awssdk.services.rekognition.model.DetectTextResponse;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.S3Object;
import software.amazon.awssdk.services.rekognition.model.TextDetection;
import software.amazon.awssdk.services.rekognition.model.TextTypes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class TextRekognizer {
  S3Client s3Client;
  RekognitionClient rekognitionClient;
  SqsClient sqsClient;
  String queueGroup = "queue-group";
  String queueName;
  String queueUrl = "";
  long sleepTime = 10 * 1000;
  String bucketName;
  Boolean threadIsBusy = false;
  Gson gson;
  Boolean didReceiveFirstMessage = false;

  public TextRekognizer(Region clientRegion, StaticCredentialsProvider staticCredentials,
      String bucketName, String queueName) {

    this.queueName = queueName;
    this.bucketName = bucketName;
    // Build the S3 Client
    this.s3Client = S3Client.builder()
        .region(clientRegion)
        .credentialsProvider(staticCredentials)
        .build();

    // Build the Rekognition Client
    this.rekognitionClient = RekognitionClient.builder()
        .region(clientRegion)
        .credentialsProvider(staticCredentials)
        .build();

    // Build the SQS client
    this.sqsClient = SqsClient.builder()
        .region(clientRegion)
        .credentialsProvider(staticCredentials)
        .build();

    this.gson = new Gson();
  }

  private void processImage(String imageKey) {
    threadIsBusy = true; // Block the thread to do it in series instead of parallel

    Image img = Image.builder().s3Object(S3Object.builder().bucket(this.bucketName).name(imageKey).build())
        .build();
    DetectTextRequest textDetectionRequest = DetectTextRequest.builder()
        .image(img)
        .build();

    DetectTextResponse textDetectionResponse = this.rekognitionClient.detectText(textDetectionRequest);
    List<TextDetection> charDetections = textDetectionResponse.textDetections();
    if (charDetections.size() != 0) {
      String text = "";

      for (TextDetection detectedChar : charDetections) {
        if (detectedChar.type().equals(TextTypes.WORD))
          text = text.concat(" " + detectedChar.detectedText());
      }
      // Append to a file
      try {
        Files.writeString(
            Path.of(System.getProperty("java.io.tmpdir"), "textOutput.txt"),
            // /tmp/textOutput.txt
            text + System.lineSeparator(),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("Error writing to file!!");
      }
    }

    threadIsBusy = false;
  }

  private void checkSqsMessages(int count) {
    System.out.println("Checking for messages! - " + count);

    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl)
        .maxNumberOfMessages(1).build();
    List<Message> messages = this.sqsClient.receiveMessage(receiveMessageRequest).messages();

    if (messages.size() > 0) {
      didReceiveFirstMessage = true;
      Message message = messages.get(0);
      String msgJson = message.body();

      Type imageRecognitionMessageType = new TypeToken<QueueMessage<ImageRecognitionMessage>>() {
      }.getType();

      QueueMessage<ImageRecognitionMessage> messageObj = new GsonBuilder().create().fromJson(msgJson,
          imageRecognitionMessageType);

      System.out.println("Processing car image with text from \"" + this.bucketName
          + "\" S3 bucket: " + messageObj.data.imageKey);

      // Remove message from the queue as we dont want others to poll this message
      DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueUrl)
          .receiptHandle(message.receiptHandle())
          .build();
      this.sqsClient.deleteMessage(deleteMessageRequest);

      processImage(messageObj.data.imageKey);
    } else {
      if (didReceiveFirstMessage) {
        System.out.println("Done processing all messages! Exiting!");
        System.exit(0);
      }
    }

  }

  private String getQueurUrl() {
    boolean queueAlreadyExists = false;

    String queueUrl = "";

    while (!queueAlreadyExists) {
      ListQueuesRequest listQueueRequest = ListQueuesRequest.builder()
          .queueNamePrefix(queueName)
          .build();

      ListQueuesResponse listQueueResponse = this.sqsClient.listQueues(listQueueRequest);

      if (listQueueResponse.queueUrls().size() > 0) {
        queueAlreadyExists = true;
        System.out.println("Queue found, Proceeding further!");
      } else {
        // Sleep for some time as we dont want flood with the request!
        try {
          System.out.println("Queue not found, waiting for " + this.sleepTime + "ms!!");
          Thread.sleep(this.sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }

    try {
      GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
          .queueName(queueName)
          .build();
      queueUrl = this.sqsClient.getQueueUrl(getQueueUrlRequest)
          .queueUrl();

      System.out.println("QueueURL: " + queueUrl);

    } catch (QueueNameExistsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }

    return queueUrl;
  }

  private void start() {
    this.queueUrl = getQueurUrl();
    int count = 0;
    while (count < 100) {

      if (threadIsBusy) {
        System.out.println("Processing image in progress, So skipping this check!");
      } else {
        count++;
        checkSqsMessages(count);
      }

      try {
        // Wait for 5sec so we dont overfload the polling queue requests
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // close all the clients
    this.s3Client.close();
    this.rekognitionClient.close();
    this.sqsClient.close();
    System.exit(0);
  }

  public static void main(String[] args) {
    System.out.println("Running the text rekognizer!!");

    String access_key_id = Constants.AWS_ACCESS_KEY_ID;
    String access_secret_key = Constants.AWS_SECRET_ACCESS_KEY;
    String bucketName = Constants.BUCKET_NAME;
    String queueName = Constants.QUEUE_NAME;

    if (access_key_id == null || access_secret_key == null || bucketName == null || queueName == null) {
      System.out.print(
          "AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY or BUCKET_NAME or QUEUE_NAME cannot be empty in the Constants.java file!");
      System.exit(1);
    }

    Region clientRegion = Region.US_EAST_1;

    AwsBasicCredentials credentials = AwsBasicCredentials.create(access_key_id,
        access_secret_key);

    StaticCredentialsProvider staticCredentials = StaticCredentialsProvider.create(credentials);

    TextRekognizer textRekognizerInstance = new TextRekognizer(clientRegion, staticCredentials, bucketName, queueName);

    textRekognizerInstance.start();
  }
}

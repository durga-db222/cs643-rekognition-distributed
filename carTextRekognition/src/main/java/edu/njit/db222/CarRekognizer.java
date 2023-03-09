package edu.njit.db222;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;

import edu.njit.db222.Utils.Constants;
import edu.njit.db222.Utils.ImageRecognitionMessage;
import edu.njit.db222.Utils.QueueMessage;
import edu.njit.db222.Utils.QueueMessageType;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsResponse;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.Label;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class CarRekognizer {
  // Clients
  S3Client s3Client;
  RekognitionClient rekognitionClient;
  SqsClient sqsClient;
  Gson gson;

  // Variables
  String bucketName;
  String labelToBeRekognized;
  Float minimumConfidenceRequired;

  String queueGroup = "queue-group";
  String queueName;
  String queueUrl = ""; // initialize on the fly

  public static void main(String[] args) {
    System.out.println("Running the car rekognition application!!");
    String aws_access_key_id = Constants.AWS_ACCESS_KEY_ID;
    String aws_access_secret_key = Constants.AWS_SECRET_ACCESS_KEY;
    String bucketName = Constants.BUCKET_NAME;
    String queueName = Constants.QUEUE_NAME;
    String labelToBeRekognized = Constants.LABEL_TO_BE_RECOGNIZED;
    Float minimumConfidenceRequired = 90.00f;

    if (aws_access_key_id == null || aws_access_secret_key == null || bucketName == null || queueName == null) {
      System.out.print(
          "AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY or BUCKET_NAME or QUEUE_NAME cannot be empty in the Constants.java file.");
      System.exit(1);
    }

    StaticCredentialsProvider credentials = StaticCredentialsProvider
        .create(AwsBasicCredentials.create(aws_access_key_id,
            aws_access_secret_key));

    CarRekognizer instance = new CarRekognizer(Region.US_EAST_1, credentials, bucketName, queueName,
        labelToBeRekognized,
        minimumConfidenceRequired);

    instance.start();
  }

  public CarRekognizer(Region clientRegion, StaticCredentialsProvider staticCredentials, String bucketName,
      String queueName,
      String labelName, Float minimumConfidenceRequired) {

    this.s3Client = S3Client.builder()
        .region(clientRegion)
        .credentialsProvider(staticCredentials)
        .build();

    this.rekognitionClient = RekognitionClient.builder()
        .region(clientRegion)
        .credentialsProvider(staticCredentials)
        .build();

    this.sqsClient = SqsClient.builder()
        .region(clientRegion)
        .credentialsProvider(staticCredentials)
        .build();

    this.gson = new Gson();

    this.bucketName = bucketName;
    this.queueName = queueName;
    this.labelToBeRekognized = labelName;
    this.minimumConfidenceRequired = minimumConfidenceRequired;
  }

  private String upsertQueue() {
    String queueUrl = "";

    try {
      ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder()
          .queueNamePrefix(queueName)
          .build();

      ListQueuesResponse listQueuesResponse = this.sqsClient.listQueues(listQueuesRequest);

      if (listQueuesResponse.queueUrls().size() == 0) {
        // No Queue Exists, So create one!
        System.out.println(queueName + " - Queue doesnt exists! Creating a new one!!");

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
            .attributesWithStrings(Map.of("FifoQueue", "true",
                "ContentBasedDeduplication", "true"))
            .queueName(queueName)
            .build();
        sqsClient.createQueue(createQueueRequest);

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
            .queueName(queueName)
            .build();

        queueUrl = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();

      } else {
        System.out.println("Queue already exists! Using the existing one!!");
        queueUrl = listQueuesResponse.queueUrls().get(0);
      }

      System.out.println("Queue URL: " + queueUrl);

      return queueUrl;
    } catch (SqsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }

    // runtime will never reach here!! due to system.exit()
    return queueUrl;
  }

  private List<String> getAllImagesListFromBucket() {
    System.out.println("Get the images from S3");
    List<String> s3ObjKeys = new ArrayList<>();

    try {
      ListObjectsRequest listObjects = ListObjectsRequest
          .builder()
          .bucket(this.bucketName)
          .build();
      ListObjectsResponse res = s3Client.listObjects(listObjects);
      List<S3Object> objects = res.contents();

      for (ListIterator<S3Object> iterVals = objects.listIterator(); iterVals.hasNext();) {
        S3Object s3Object = (S3Object) iterVals.next();
        System.out.println("Image found in njit-cs-643 S3 bucket: " + s3Object.key());

        s3ObjKeys.add(s3Object.key());
      }

      System.out.println("---Got all the images from S3----");

    } catch (S3Exception e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }

    return s3ObjKeys;
  }

  private void rekognizeImageAndAddToQueue(String labelToBeRekognized, Float minimumConfidenceRequired,
      List<String> imagesKeyList) {
    System.out.println(
        "Starting to rekognize images and adding to queue if the label \"" + labelToBeRekognized
            + "\" is found with a minimum confidence of "
            + minimumConfidenceRequired + " !!!");

    for (String imageKey : imagesKeyList) {
      // Recognize the image using aws rekognition
      Image img = Image.builder().s3Object(software.amazon.awssdk.services.rekognition.model.S3Object
          .builder().bucket(this.bucketName).name(imageKey).build())
          .build();

      DetectLabelsRequest detectLabelRequest = DetectLabelsRequest.builder().image(img)
          .minConfidence(minimumConfidenceRequired)
          .build();

      DetectLabelsResponse detectionResult = this.rekognitionClient.detectLabels(detectLabelRequest);
      List<Label> labels = detectionResult.labels();

      for (Label label : labels) {
        if (label.name().toLowerCase().equals(labelToBeRekognized)) {
          if (this.queueUrl.length() == 0) {
            // Queue url doesnt exists, Create one or get existing!!
            this.queueUrl = upsertQueue();
          }

          System.out.println("----Pushing image " + imageKey + " into the queue for text rekognition!!---");

          QueueMessage<ImageRecognitionMessage> newMessage = new QueueMessage<ImageRecognitionMessage>(
              QueueMessageType.ImageRecognition,
              new ImageRecognitionMessage(imageKey));

          String serializedMessage = this.gson.toJson(newMessage);

          // Send the image for further processing!!
          this.sqsClient
              .sendMessage(SendMessageRequest.builder().messageGroupId(this.queueGroup).queueUrl(this.queueUrl)
                  .messageBody(serializedMessage).build());

          // Dont wanna check anymore labels for this image, so break!!!
          break;
        }
      }
    }
  }

  private void start() {
    // Get the image list
    List<String> imagesKeyList = getAllImagesListFromBucket();

    // Rekognize the images
    rekognizeImageAndAddToQueue(this.labelToBeRekognized,
        this.minimumConfidenceRequired, imagesKeyList);

    // Close the clients
    this.s3Client.close();
    this.rekognitionClient.close();
    this.sqsClient.close();
    System.exit(0);
  }
}

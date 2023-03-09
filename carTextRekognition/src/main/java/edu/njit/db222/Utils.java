package edu.njit.db222;

class Utils {
  // ----- Data obj for SQS
  static enum QueueMessageType {
    ImageRecognition
  }

  static class QueueMessage<T> {
    public final QueueMessageType type;
    public T data;

    public QueueMessage(QueueMessageType type, T data) {
      this.type = type;
      this.data = data;
    }
  }

  static class ImageRecognitionMessage {
    public final String imageKey;

    public ImageRecognitionMessage(String imageKey) {
      this.imageKey = imageKey;
    }
  }

  // ----- Data obj for SQS

  // ----- Constants with AWS credentials
  static public class Constants {
    static String AWS_ACCESS_KEY_ID = "AKIAYEUNOJNEZR25SAGG";
    static String AWS_SECRET_ACCESS_KEY = "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";
    static String BUCKET_NAME = "njit-cs-643";
    static String QUEUE_NAME = "car-images-queue.fifo";
    static String LABEL_TO_BE_RECOGNIZED = "car";
  }

}
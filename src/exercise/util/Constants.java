package exercise.util;

public class Constants {
  public static final int SECONDS_TO_RUN = 5;
  public static final String INCOMING_REQUEST_STREAM_NAME = "IncomingCabRequests";
  public static final String DRIVER_LOCATION_STREAM_NAME = "DriverLocationUpdates";
  public static final String REGION = "eu-central-1";
  public static final int MAX_DRIVER_COUNT = 150;
  public static final int MIN_DRIVER_COUNT = 50;
  public static final String INCOMING_REQUEST_APPLICATION_NAME = "IncomingRequestsConsumer";
  public static final String DRIVER_LOCATION_APPLICATION_NAME = "IncomingRequestsConsumer";

  /**
   * Redis Connection config
   */
  public static final String REDIS_HOST =
      "exercise-redis.i7wvyl.clustercfg.euc1.cache.amazonaws.com";
  public static final Integer REDIS_PORT = 6379;
}

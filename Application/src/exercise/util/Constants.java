package exercise.util;

import java.util.concurrent.TimeUnit;

public class Constants {
  /**
   * Stream Names
   */
  public static final String INCOMING_REQUEST_STREAM_NAME = "IncomingCabRequests";
  public static final String DRIVER_LOCATION_STREAM_NAME = "DriverLocationUpdates";
  public static final String TRIP_DETAIL_STREAM_NAME = "TripDetailUpdates";
  /**
   * Consumer Applications
   */
  public static final String INCOMING_REQUEST_APPLICATION_NAME = "IncomingRequestsConsumer";
  public static final String DRIVER_LOCATION_APPLICATION_NAME = "DriverLocationsConsumer";
  public static final String TRIP_DETAIL_APPLICATION_NAME = "TripDetailsConsumer";
  /**
   * AWS Config
   */
  public static final String REGION = "eu-central-1";
  /**
   * Constant Values
   */
  public static final int MAX_DRIVER_COUNT = 1000;
  public static final int MIN_DRIVER_COUNT = 100;
  public static final double MIN_SURGE_MULTIPLIER = 1;
  public static final double MAX_SURGE_MULTIPLIER = 5;
  public static final double LATITUDE_ERROR = 0.0027 * 2;
  public static final double LONGITUDE_ERROR = 0.0055 * 2;
  /**
   * Time Interval Constants
   */
  public static final int SECONDS_TO_RUN = 5;
  public static final Integer SCHEDULED_TIME_INTERVAL = 10;
  public static final TimeUnit SCHEDULED_TIME_UNIT = TimeUnit.MINUTES;
  public static final long DRIVER_LOCATION_UPDATE_INTERVAL = 20;
  public static final long DRIVER_LOCATION_UPDATE_INITIAL_DELAY = 1;
  public static final long INCOMING_REQUEST_INTERVAL = 20;
  public static final long INCOMING_REQUEST_INITIAL_DELAY = 1;
  public static final long TRIP_DETAIL_INTERVAL = 20;
  public static final long TRIP_DETAIL_INITIAL_DELAY = 1;
  /**
   * String Constants
   */
  public static final String DEMAND_KEY_PREFIX = "demand_";
  public static final String SUPPLY_KEY_PREFIX = "supply_";
  public static final String SURGE_PRICING_KEY_PREFIX = "surge_";
  public static final String DELIMITER = ";;;;";
  /**
   * Data Set Paths
   */
  private static final String DATA_SET_FOLDER_PATH = "../../Datasets/";
  public static final String INCOMING_REQUESTS_DATA_SET_FILE_PATH =
      DATA_SET_FOLDER_PATH + "IncomingRequests.csv";
  public static final String DRIVER_LOCATIONS_DATA_SET_FILE_PATH =
      DATA_SET_FOLDER_PATH + "DriverLocations.csv";
  public static final String TRIP_DETAILS_DATA_SET_FILE_PATH =
      DATA_SET_FOLDER_PATH + "TripDetails.csv";
  /**
   * Redis Connection config
   */
  public static final String REDIS_HOST = "localhost";
  public static final Integer REDIS_PORT = 6379;
  /**
   * AWS RDS MySQL Connection config
   */
  public static final String DB_HOST =
      "grab-exercise-db-instance.coooavv2qtha.eu-central-1.rds.amazonaws.com";
  public static final Integer DB_PORT = 3306;
  public static final String DB_NAME = "GrabExercise";
  public static final String DB_USER_NAME = "grabDB";
  public static final String DB_PASSWORD = "mainkyodassa";
  public static final String DB_DRIVER_NAME = "mysql";
  public static final String DB_TABLE_NAME = "GrabExerciseTable";
}

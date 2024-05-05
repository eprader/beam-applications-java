package org.example;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.factory.Event;
import org.example.factory.EventConsumer;
import org.example.factory.EventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOG = LoggerFactory.getLogger("APP");

  public static void main(String[] args) {
    String BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVER");
    String application = System.getenv("APPLICATION");
    String datasetType = System.getenv("DATASET");
    double scalingFactor = Double.parseDouble(System.getenv("SCALING"));
    long repetitions = Long.parseLong(System.getenv("REP"));
    long duplicationFactor = Long.parseLong(System.getenv("DUP"));
    String topic = System.getenv("TOPIC");
    String mode = System.getenv("MODE");

    if (BOOTSTRAP_SERVERS == null
        || BOOTSTRAP_SERVERS.isEmpty()
        || application == null
        || application.isEmpty()
        || datasetType == null
        || datasetType.isEmpty()
        || scalingFactor == 0.0
        || repetitions < 0
        || duplicationFactor < 0
        || topic == null
        || topic.isEmpty()
        || mode.isEmpty()) {

      System.err.println("One or more required environment variables are not set.");
      // You can choose to exit the application or handle the error gracefully
      // System.exit(1);
      return;
    }

    System.out.println("Bootstrap server " + BOOTSTRAP_SERVERS);
    if (mode.equals("R")) {
      Producer producer = new Producer(BOOTSTRAP_SERVERS, LOG);
      EventGen eventGen =
          new EventGen(scalingFactor, producer, topic, repetitions, duplicationFactor);

      if (application.equals("train")) {
        switch (datasetType) {
          case "SYS":
            {
              String csvFileName =
                  "/opt/kafkaproducer/resources/train/inputFileForTimerSpout-CITY.csv";
              eventGen.launch(csvFileName);
              break;
            }
          case "FIT":
            {
              String csvFileName =
                  "/opt/kafkaproducer/resources/train/inputFileForTimerSpout-FIT.csv";
              eventGen.launch(csvFileName);
              break;
            }
          case "TAXI":
            {
              String csvFileName =
                  "/opt/kafkaproducer/resources/train/inputFileForTimerSpout-TAXI.csv";
              eventGen.launch(csvFileName);
              break;
            }
          default:
            throw new RuntimeException("No valid dataset given: " + datasetType);
        }
      } else if (application.equals("pred")) {
        String csvFileName;
        switch (datasetType) {
          case "SYS":
            {
              csvFileName = "/opt/kafkaproducer/resources/pred/SYS_sample_data_senml.csv";
              break;
            }
          case "FIT":
            {
              csvFileName = "/opt/kafkaproducer/resources/pred/FIT_sample_data_senml.csv";
              break;
            }
          case "TAXI":
            {
              csvFileName = "/opt/kafkaproducer/resources/pred/TAXI_sample_data_senml.csv";
              break;
            }
          default:
            throw new RuntimeException("No valid dataset given: " + datasetType);
        }
        if (csvFileName.contains("senml")) {
          eventGen.launch(csvFileName, true);
        } else {
          eventGen.launch(csvFileName);
        }
      }
    } else if (mode.equals("E")) {
      CSVParser parser = new CSVParserBuilder().withSeparator('|').build();
      BlockingQueue<Event> eventQueue = new DelayQueue<>();
      CSVReader reader;

      String csvFileName = "";
      if (application.equals("pred")) {
        csvFileName = "/opt/kafkaproducer/resources/pred/test_taxi_500.csv";
      }
      if (application.equals("train")) {
        csvFileName = "/opt/kafkaproducer/resources/train/test_input_SYS.csv";
      }
      try {
        reader =
            new CSVReaderBuilder(new FileReader(csvFileName))
                .withSkipLines(1)
                .withCSVParser(parser)
                .build();
        EventProducer producer = new EventProducer(eventQueue, reader);
        producer.run();
      } catch (FileNotFoundException e) {
        System.out.println("file_not_found");
      }
      Properties props = new Properties();
      props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
      props.put("key.serializer", LongSerializer.class.getName());
      props.put("value.serializer", StringSerializer.class.getName());

      EventConsumer consumer = new EventConsumer(eventQueue, props, topic);
      consumer.run();

    } else {
      throw new RuntimeException("Illegal mode argument");
    }
  }
}
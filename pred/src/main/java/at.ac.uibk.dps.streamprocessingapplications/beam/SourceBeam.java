package at.ac.uibk.dps.streamprocessingapplications.beam;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singleton;

import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.kafka.MyKafkaConsumer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceBeam extends DoFn<String, SourceEntry> implements ISyntheticEventGen {

  private static Logger l;

  BlockingQueue<List<String>> eventQueue;
  String csvFileName;
  String outSpoutCSVLogFileName;
  String experiRunId;
  long msgId;

  long numberLines;

  private final long POLL_TIMEOUT_MS = 1000;

  private final String datasetType;

  private MyKafkaConsumer myKafkaConsumer;

  public static void initLogger(Logger l_) {
    l = l_;
  }

  public SourceBeam(
      String csvFileName,
      String outSpoutCSVLogFileName,
      String experiRunId,
      long lines,
      String bootstrap,
      String topic,
      String datasetType) {
    this.csvFileName = csvFileName;
    this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
    this.experiRunId = experiRunId;
    this.myKafkaConsumer =
        new MyKafkaConsumer(bootstrap, "group-" + UUID.randomUUID(), 10000, topic);
    this.numberLines = lines;
    this.datasetType = datasetType;
  }

  public SourceBeam(
      String csvFileName,
      String outSpoutCSVLogFileName,
      long lines,
      String bootstrap,
      String topic,
      String datasetType) {
    this(csvFileName, outSpoutCSVLogFileName, "", lines, bootstrap, topic, datasetType);
  }

  private long extractTimeStamp(String row) {
    Gson gson = new Gson();
    JsonArray jsonArray = gson.fromJson(row, JsonArray.class);

    String pickupDatetime = null;
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
      if (jsonObject.has("n") && jsonObject.get("n").getAsString().equals("pickup_datetime")) {
        pickupDatetime = jsonObject.get("vs").getAsString();
        break;
      }
    }

    return convertToUnixTimeStamp(pickupDatetime);
  }

  private long convertToUnixTimeStamp(String dateString) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    long unixTimestampSeconds = 0;
    try {
      Date date = dateFormat.parse(dateString);
      long unixTimestamp = date.getTime();
      unixTimestampSeconds = unixTimestamp / 1000;
    } catch (ParseException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return unixTimestampSeconds;
  }

  @Setup
  public void setup() {
    Random r = new Random();
    try {
      msgId = (long) (1 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));

    } catch (Exception e) {

      e.printStackTrace();
    }
    // this.eventGen = new EventGen(this, this.scalingFactor);
    // this.eventQueue = new LinkedBlockingQueue<>();
    // String uLogfilename = this.outSpoutCSVLogFileName + msgId;
    boolean isJson = csvFileName.contains("senml");
    initLogger(LoggerFactory.getLogger("APP"));

    // this.eventGen.launch(this.csvFileName, uLogfilename, -1, isJson); // Launch threads
  }

  @ProcessElement
  public void processElement(@Element String input, OutputReceiver<SourceEntry> out)
      throws IOException {
    long count = 1, MAX_COUNT = 100; // FIXME?
    KafkaConsumer<Long, byte[]> kafkaConsumer;
    kafkaConsumer = myKafkaConsumer.createKafkaConsumer();
    kafkaConsumer.subscribe(singleton(myKafkaConsumer.getTopic()), myKafkaConsumer);

    while (count < numberLines) {
      /*
      List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
      if (entry == null) {
          // return;
          continue;
      }
       */

      try {
        ConsumerRecords<Long, byte[]> records = kafkaConsumer.poll(ofMillis(POLL_TIMEOUT_MS));
        if (!records.isEmpty()) {
          for (ConsumerRecord<Long, byte[]> record : records) {
            SourceEntry values = new SourceEntry();
            String rowString = new String(record.value());
            String newRow;
            if (datasetType.equals("TAXI")) {
              newRow = "{\"e\":" + rowString + ",\"bt\":" + extractTimeStamp(rowString) + "}";
            } else if (datasetType.equals("FIT")) {
              newRow = "{\"e\":" + rowString + ",\"bt\": \"1358101800000\"}";

            } else {
              newRow = "{\"e\":" + rowString + ",\"bt\":1358101800000}";
            }
            l.info(newRow);
            values.setMsgid(Long.toString(msgId));
            values.setPayLoad(newRow);
            out.output(values);
            msgId++;
            count++;
          }
        }
      } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
        // Handle invalid offset or no offset found errors when auto.reset.policy is not set
        System.out.println("Invalid or no offset found, and auto.reset.policy unset, using latest");
        throw new RuntimeException(e);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void receive(List<String> event) {
    try {
      this.eventQueue.put(event);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
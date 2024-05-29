package at.ac.uibk.dps.streamprocessingapplications.eventGenerators.KafkaProducer;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class EventProducer implements Runnable {
  private BlockingQueue<Event> eventQueue;
  private CSVReader reader;

  public EventProducer(BlockingQueue<Event> eventQueue, CSVReader reader) {
    this.eventQueue = eventQueue;
    this.reader = reader;
  }

  @Override
  public void run() {
    try {
      String[] lineInArray;
      while ((lineInArray = reader.readNext()) != null) {
        Event newEvent =
            new Event(lineInArray[1], System.currentTimeMillis() + Long.parseLong(lineInArray[0]));
        eventQueue.add(newEvent);
      }
    } catch (CsvValidationException e) {
      System.out.println("csv validation");
    } catch (IOException e) {
      System.out.println("io_exc");
    }
    // System.out.println("done");
  }
}

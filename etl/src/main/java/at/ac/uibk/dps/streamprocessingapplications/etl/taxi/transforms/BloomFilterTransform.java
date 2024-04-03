package at.ac.uibk.dps.streamprocessingapplications.etl.taxi.transforms;

import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model.TaxiRide;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BloomFilterTransform extends PTransform<PCollection<TaxiRide>, PCollection<TaxiRide>> {

  private static final Logger LOGGER = LogManager.getLogger(BloomFilterTransform.class);
  Optional<BloomFilter<String>> bloomFilter;

  public BloomFilterTransform() {
    this.bloomFilter = buildBloomFilter();
  }

  @Override
  public PCollection<TaxiRide> expand(PCollection<TaxiRide> input) {
    if (this.bloomFilter.isEmpty()) {
      LOGGER.info("Bloom filter not available. Falling back to no op.");
      return input;
    }
    return input.apply(
        MapElements.into(TypeDescriptor.of(TaxiRide.class))
            .via(
                ride -> {
                  return ride;
                }));
  }

  private Optional<BloomFilter<String>> buildBloomFilter() {
    /* WARN:
     * Do NOT replace this funnel! It has to be the same as the one used to create the serialized model.
     * The bloom filter model file was adopted from the `riot-bench` repository
     * and this is their funnel implementation.
     */
    final Funnel<String> funnel = (memberId, sink) -> sink.putString(memberId, Charsets.UTF_8);
    final String MODEL_FILE_PATH = "bloomfilter-TAXI.model";
    ClassLoader classLoader = getClass().getClassLoader();
    BloomFilter<String> bloomFilter;

    try (InputStream inputStream = classLoader.getResourceAsStream(MODEL_FILE_PATH)) {
      bloomFilter = BloomFilter.readFrom(inputStream, funnel);
    } catch (IOException e) {
      LOGGER.error(String.format("Unable to build bloom filter from path: %s", MODEL_FILE_PATH));
      return Optional.empty();
    }

    return Optional.of(bloomFilter);
  }
}

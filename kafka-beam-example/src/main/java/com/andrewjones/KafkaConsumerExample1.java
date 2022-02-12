package com.andrewjones;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerExample1 {
    public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    options.setJobName("kafka-word-count");

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    p.apply(
        KafkaIO.<String, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("stocks")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withoutMetadata())
        .apply(Values.create())
        // Apply a FlatMapElements transform the PCollection of text lines.
        // This transform splits the lines in PCollection<String>, where each element is an
        // individual word in Shakespeare's collected texts.
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
        // We use a Filter transform to avoid empty word
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        // Apply the Count transform to our PCollection of individual words. The Count
        // transform returns a new PCollection of key/value pairs, where each key represents a
        // unique word in the text. The associated value is the occurrence count for that word.
        .apply(Count.perElement())
        .apply(MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via(kv -> KV.of(kv.getKey(), String.valueOf(kv.getValue()))))
        .apply(KafkaIO.<String, String>write()
            .withBootstrapServers("localhost:9092")
            .withTopic("word-count")
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class));

    //For yarn, we don't need to wait after submitting the job,
    //so there is no need for waitUntilFinish(). Please use
    //p.run()
    p.run().waitUntilFinish();
  }
/*
    static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);

        p.apply(KafkaIO.<Long, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("stocks")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                // the first 5 records.
                // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                .withMaxNumRecords(5)
                .withoutMetadata() // PCollection<KV<Long, String>>
        )
        .apply(Values.<String>create())
        .apply(TextIO.write().to("/tmp/wordcounts"));

        p.run().waitUntilFinish();
    }
    */
}

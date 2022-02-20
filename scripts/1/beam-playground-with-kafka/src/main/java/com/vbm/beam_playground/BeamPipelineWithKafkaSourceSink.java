package com.vbm.beam_playground;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.repackaged.beam_sdks_java_core.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaIO.Write;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;

public class BeamPipelineWithKafkaSourceSink {

	private transient static Schema transAvroSchema;

	public static Pipeline createBeamPipeline(final Schema avroSchema) {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);

		org.apache.beam.sdk.schemas.Schema rowSchema = AvroUtils.toBeamSchema(avroSchema);
		transAvroSchema = avroSchema;

		KafkaIO.Read<String, GenericRecord> kafkaIoRead = KafkaIO.<String, GenericRecord>read()
				.withBootstrapServers("localhost:9092").withTopic("stocksAvro")
				.withKeyDeserializer(StringDeserializer.class)
				.withValueDeserializerAndCoder(GenericAvroDeserializer.class,
						AvroCoder.of(GenericRecord.class, avroSchema))
				.updateConsumerProperties(ImmutableMap.of("schema.registry.url", "http://localhost:8081"));

		final Write<Void, GenericRecord> kafkaIoWrite = KafkaIO.<Void, GenericRecord>write()
				.withBootstrapServers("localhost:9092").withTopic("stocks2")
				.withValueSerializer(GenericAvroSerializer.class)
				.updateProducerProperties(ImmutableMap.of("schema.registry.url", "http://localhost:8081"));

		pipeline.apply(kafkaIoRead)
				.apply(ParDo.of(new DoFn<KafkaRecord<String, GenericRecord>, Row>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						GenericRecord record = c.element().getKV().getValue();
						Row r = AvroUtils.toBeamRowStrict(record, rowSchema);
						c.output(r);
					}}))
				.apply(ParDo.of(new DoFn<Row, GenericRecord>() {
						@ProcessElement
						public void processElement(ProcessContext c) {
							GenericRecord genericRecord = AvroUtils.toGenericRecord(c.element(), transAvroSchema);
							c.output(genericRecord);
						}
					})).setCoder(AvroCoder.of(GenericRecord.class, transAvroSchema))
				.apply(kafkaIoWrite.values());

		return pipeline;
	}
}

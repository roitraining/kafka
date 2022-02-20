package com.vbm.beam_playground;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.repackaged.beam_sdks_java_core.net.bytebuddy.utility.RandomString;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.RandomUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.namespace.Simple;
import com.namespace.Simple2;

public class ComplexDataProducer {

	public static void main(String[] args) throws IOException, InterruptedException {

		ClassLoader loader = ComplexDataProducer.class.getClassLoader();
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(loader.getResourceAsStream("complex-schema.avsc"));

		Properties my_props = new Properties();
		my_props.load(loader.getResourceAsStream("Producer.prop"));

		Thread pipelineThread = new Thread(new Runnable() {
			Pipeline pipeline = BeamPipelineWithKafkaSourceSink.createBeamPipeline(schema);

			@Override
			public void run() {
				pipeline.run();

			}
		});
		pipelineThread.start();
		sendDataToKafka(schema, my_props);

		pipelineThread.join();

	}

	private static void sendDataToKafka(Schema schema, Properties my_props) throws IOException, InterruptedException {
		try (Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(my_props)) {
			for (int i = 1000; i < 2000; i++) {
				Thread.sleep(1000);
				GenericRecord datum = generateComplexData(schema);
				producer.send(new ProducerRecord<String, GenericRecord>("complex", String.valueOf(i), datum));
			}
		}
	}
	
	/*
	 {
		"cmplx1": {
			"req1": "enim laboris mi",
			"opt1": "eu veniam amet",
			"opt2": true,
			"opt3": 86585182.74703905
		},
		"optcmplx": {
			"s": true
		},
		"opt2": false,
		"opt3": [-1894889953, -1583251848, 40949557]
	 }
	*/

	private static GenericRecord generateComplexData(Schema schema) {
		GenericRecord datum = new GenericData.Record(schema);

		Simple simpleObj = generateSimpleType1data();
		Simple2 simpleType2Obj = generateSimpleType2data();

		List<Number> randomListOfNumbers = generateRandomListOfNumbers();

		datum.put("cmplx1", simpleObj);
		datum.put("optcmplx", simpleType2Obj);
		datum.put("opt2", RandomUtils.nextBoolean());
		datum.put("opt3", randomListOfNumbers);
		return datum;
	}

	private static List<Number> generateRandomListOfNumbers() {
		List<Number> numberList = new ArrayList<>();
		for (int i = 0; i < RandomUtils.nextInt(2, 20); i++) {
			numberList.add(RandomUtils.nextFloat());
		}
		return numberList;
	}

	private static Simple generateSimpleType1data() {
		Simple simpleObj = new Simple();
		simpleObj.setReq1(RandomString.make(20));
		simpleObj.setOpt1(RandomString.make(10));
		simpleObj.setOpt2(RandomUtils.nextBoolean());
		simpleObj.setOpt3(RandomUtils.nextDouble());
		return simpleObj;
	}

	private static Simple2 generateSimpleType2data() {
		Simple2 simpleType2Obj = new Simple2();
		simpleType2Obj.setS(true);
		return simpleType2Obj;
	}
}
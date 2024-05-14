package com.sdg.ingestion.workers.pipeline.sinks.impl;

import com.sdg.ingestion.config.dataflowSettings.sink.Sink;
import com.sdg.ingestion.workers.pipeline.sinks.SinkFactory;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.*;

public class SinkKafkaFlow extends SinkFactory {
    private final Sink sink;
    private static SparkSession spark;

    private final static Logger LOG = LoggerFactory.getLogger(SinkKafkaFlow.class);
    // private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private final static String BOOTSTRAP_SERVERS = "broker:9092,broker:9093,broker:9094";

    public SinkKafkaFlow(SparkSession spark, Sink sink) {
        this.sink = sink;
        this.spark = spark;
    }


    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public Dataset read() { return null; }

    public Dataset transformation(Dataset dataset){
        System.out.println("-------------------------------------> Sending!!");
        List<String> elementsToSend = dataset.toJSON().collectAsList();
        try {
            sendMessage(elementsToSend, sink.getTopics());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("-------------------------------------> Message sent successfully");
        return dataset;
    }

    public Map<String, Dataset> run(Map<String, Dataset> datasetMap) {
        LOG.info("-----------------> Running SinkJSONFlow");
        //get input dataset
        Dataset nDataset = datasetMap.get(sink.getInput());
        nDataset.show(false);
        if (nDataset == null) {
            nDataset = this.read();
        }
        this.transformation(nDataset);
        return datasetMap;
    }

    static void sendMessage(List<String> messages, List<String> topics) throws InterruptedException {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(5);
        try {
            for(int messageCount=0; messageCount < messages.size(); messageCount++) {
                for(int topicCount=0; topicCount < topics.size(); topicCount++) {
                    final ProducerRecord<Long, String> record = new ProducerRecord(topics.get(topicCount), Long.parseLong("1"), messages.get(messageCount));
                    producer.send(record, (metadata, exception) -> {
                        long elapsedTime = System.currentTimeMillis() - time;
                        if (metadata != null) {
                            System.out.printf(
                                    "sent record(key=%s value=%s) meta(partition=%d, offset=%d) time=%d\n",
                                    record.key(),
                                    record.value(),
                                    metadata.partition(),
                                    metadata.offset(),
                                    elapsedTime
                            );
                        } else {
                            exception.printStackTrace();
                        }
                        countDownLatch.countDown();
                    });
                }
            }
        }finally {
            producer.close();
        }
    }
}

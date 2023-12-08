package com.services.streamsDemo.TopologyStreams;

import com.services.streamsDemo.Joins.Streamjoins;
import com.services.streamsDemo.avro.schema.EmpFullDetails;
import com.services.streamsDemo.avro.schema.EmployeeAddress;
import com.services.streamsDemo.avro.schema.EmployeePersonal;
import com.services.streamsDemo.avro.schema.EmployeeVehicle;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.time.Duration;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class Streamstopology {

    public void run() throws Exception{
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-stream-join-app");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Serdes for Avro types
        Serde<EmployeeAddress> addressSerde = new SpecificAvroSerde<>();
        Serde<EmployeePersonal> personalSerde = new SpecificAvroSerde<>();
        Serde<EmployeeVehicle> vehicleSerde = new SpecificAvroSerde<>();
        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();
        // Define input topics
        KStream<String, EmployeeAddress> addressKStream = builder.stream("Address-Details", Consumed.with(Serdes.String(), addressSerde));
        KStream<String, EmployeePersonal> personalKStream = builder.stream("Personal-Details", Consumed.with(Serdes.String(), personalSerde));
        KStream<String, EmployeeVehicle> vehicleKStream = builder.stream("Vehicle-Details", Consumed.with(Serdes.String(), vehicleSerde));
        personalKStream.peek((k,v)-> System.out.println(v.getEmpId()));
        //Perform the outer-join
        KStream<Integer, EmpFullDetails> outerJoinedStream=personalKStream.outerJoin(
                        addressKStream,
                        Streamjoins::setempFullDetails,
                        JoinWindows.of(Duration.ofMillis(1000)))
                .selectKey((k,v) -> v.getEmpId())
                .groupByKey()
                .aggregate(EmpFullDetails::new,
                        (aggKey,oldValue,newValue) -> Streamjoins.aggregateSet(oldValue,newValue),
                        Materialized.as("queryable-store-name"))
                .toStream();

        System.out.println("Stream After Outer-Join Operation:"+outerJoinedStream);

        // Build and start the Kafka Streams application
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();
    }

}

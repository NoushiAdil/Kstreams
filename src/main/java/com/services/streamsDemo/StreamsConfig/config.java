package com.services.streamsDemo.StreamsConfig;

import com.services.streamsDemo.StreamsProcessor.Streamjoins;
import com.services.streamsDemo.avro.schema.EmpFullDetails;
import com.services.streamsDemo.avro.schema.EmployeeAddress;
import com.services.streamsDemo.avro.schema.EmployeePersonal;
import com.services.streamsDemo.avro.schema.EmployeeVehicle;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class config {
    @Value("${address.topic.name}")
    private String addressTopic;
    @Value("${personal.topic.name}")
    private String personalTopic;
    @Value("$vehicle.topic.name}")
    private String vehicleTopic;
    @Value("$full-details.topic.name}")
    private String fullDetailsTopic;
//    @Value("$spring.main.allow-bean-definition-overriding}")
//    private String overRiding;
//    @Autowired private KafkaProperties kafkaProperties;
//    //(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
//    @Autowired
//    private KafkaStreamsDefaultConfiguration kafkaStreamsDefaultConfiguration;

//    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
//    public StreamsBuilderFactoryBean kStreamsBuilder() {
//        return new StreamsBuilderFactoryBean();
//    }

//    public KafkaStreams kafkaStreams(@Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME) StreamsBuilder streamsBuilder,
//                                   @Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME) StreamsBuilderFactoryBean streamsBuilderFactoryBean)
     @Bean
     public KafkaStreams kafkaStreams(KafkaProperties kafkaProperties) {
        final Properties streamsConfiguration = new Properties();
            // Give the Streams application a unique name.
            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-stream-join-app");
            // Where to find Kafka broker(s).
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            // Specify default (de)serializers for record keys and for record values.
            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
            // Where to find the Confluent schema registry instance(s)
            streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
            streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // build the topology
            Topology topology = buildTopology(new StreamsBuilder(),singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"));
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
         //Build and start the Kafka Streams application
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                kafkaStreams.close();
            } catch (final Exception e) {
                // ignored
            }
        }));
        return kafkaStreams;
    }

    public Topology buildTopology(StreamsBuilder builder,final Map<String, String> serdeConfig){
        // declare Avro Serde for all topics
        final SpecificAvroSerde<EmployeeAddress> addressSpecificAvroSerde = new SpecificAvroSerde<>();
        addressSpecificAvroSerde.configure(serdeConfig, false);
        final SpecificAvroSerde<EmployeePersonal> personalSpecificAvroSerde = new SpecificAvroSerde<>();
        personalSpecificAvroSerde.configure(serdeConfig, false);
        final SpecificAvroSerde<EmployeeVehicle> vehicleSpecificAvroSerde = new SpecificAvroSerde<>();
        vehicleSpecificAvroSerde.configure(serdeConfig, false);
        // read the address stream
        final KStream<String, EmployeeAddress> addressStream = builder.stream(addressTopic, Consumed.with(Serdes.String(), addressSpecificAvroSerde));
        addressStream.print(Printed.<String, EmployeeAddress>toSysOut().withLabel("Address Stream"));
        // read the personal stream
        final KStream<String, EmployeePersonal> personalStream = builder.stream(personalTopic, Consumed.with(Serdes.String(), personalSpecificAvroSerde));
        personalStream.print(Printed.<String, EmployeePersonal>toSysOut().withLabel("Personal Stream"));
        // read the vehicle stream
        final KStream<String, EmployeeVehicle> vehicleStream = builder.stream(vehicleTopic, Consumed.with(Serdes.String(), vehicleSpecificAvroSerde));
        vehicleStream.print(Printed.<String, EmployeeVehicle>toSysOut().withLabel("Vehicle Stream"));
        // join the addressStream and vehiclestream to create outerjoinedstream
        KStream<Integer, EmpFullDetails> outerJoinedStream=personalStream.outerJoin(
				addressStream,
				Streamjoins::setempFullDetails,
				JoinWindows.of(Duration.ofMillis(1000)))
				.selectKey((k,v) -> v.getEmpId())
				.groupByKey()
				.aggregate(EmpFullDetails::new,
						(aggKey,oldValue,newValue) -> Streamjoins.aggregateSet(oldValue,newValue),
						Materialized.as("queryable-store-name"))
				 .toStream();

        outerJoinedStream.print(Printed.<Integer,EmpFullDetails>toSysOut().withLabel("Outer Joined Avro Stream"));

        // finish the topology
        return builder.build();
    }
//

}

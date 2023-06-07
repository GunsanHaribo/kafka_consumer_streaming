package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class Duplication_ad_filter {

    private static String APPLICATION_NAME = "streams-filter-application";
    private static String BOOTSTRAP_SERVERS = "3.35.96.120:9092";
    private static String Instar_ph= "instar_ph.kafka";
    private static String Previous_ph = "previous_ph.kafka";
    private static String Instar_co= "instar_co.kafka";
    private static String Previous_co= "previous_co.kafka";
    private static String Instar_delete_ad = "instar_delete_ad.kafka";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/ec2-user/kafka_2.12-2.5.0/streaming_meta");
        
        StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> input_stream_ph = builder.stream(Instar_ph);
        KTable<String, String> previous_table_ph = builder.table(Previous_ph);

         KStream<String, String> input_stream_co = builder.stream(Instar_co);
         KTable<String, String> previous_table_co = builder.table(Previous_co);


        KStream<String, String> joinedStream_ph = input_stream_ph.leftJoin(previous_table_ph, (leftValue, rightValue)-> {
            if (rightValue == null){
               
                return leftValue;
            }else {
                
                return null;
            }
        
        });

        KStream<String, String> filtered_null_Stream_ph = joinedStream_ph.filter((key, value) -> value != null);


        KStream<String, String> joinedStream_co = input_stream_co.leftJoin(previous_table_co, (leftValue, rightValue)-> {
            if (rightValue == null){
               
                return leftValue;
            }else {
                
                return null;
            }
        
        });
        
        KStream<String, String> filtered_null_Stream_co = joinedStream_co.filter((key, value) -> value != null);

        
        filtered_null_Stream_ph.to(Previous_ph);
        filtered_null_Stream_co.to(Previous_co);

        filtered_null_Stream_ph.filter((key, value) -> !value.contains("할인") && !value.contains("이벤트") && !value.contains("혜택") && !value.contains("광고") &&!value.contains("리그램") &&!value.contains("협찬") &&!value.contains("호빠")  &&!value.contains("kissing") &&!value.contains("마케팅")&&!value.contains("SIZE")).to(Instar_delete_ad);
        filtered_null_Stream_co.filter((key, value) -> !value.contains("할인") && !value.contains("이벤트") && !value.contains("혜택") && !value.contains("광고") &&!value.contains("리그램") &&!value.contains("협찬") &&!value.contains("호빠") &&!value.contains("kissing") &&!value.contains("마케팅") &&!value.contains("SIZE")).to(Instar_delete_ad);
        
  
        KafkaStreams streams;
        streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

  
}


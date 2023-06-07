package org.example;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.time.DayOfWeek;
import java.time.Duration;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;


import java.util.*;

import org.json.JSONException;
import com.vdurmont.emoji.EmojiParser;

import java.sql.Timestamp;
import java.time.*;



public class Test_cosumer2 {
    private static final int BUFFER_SIZE = 20; // 이상 넘어가야 출력 
    private static final String bucketName = "seoultechs3";
    private static final  S3Client s3Client = S3Client.builder()
                .region(Region.of("ap-northeast-2"))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    "AKIA6JHHZ3L62NLPACGI",
                    "o9M0M9lGxL75OD5INMCpfrEfEztWNdugon7xVdw9"
                )))
                .build();

    static Timestamp timestamp;
    static Instant instant;
    static LocalDateTime localDateTime;
    static DayOfWeek dayOfWeek;
    static String dayOfWeekString;

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "3.35.96.120:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "s3-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        consumer.subscribe(Collections.singletonList("instar_delete_ad.kafka"));


        String JMT[] = {"여의도맛집", "노원맛집", "강남맛집", "용산맛집", "을지로맛집", "연남맛집", "잠실맛집", "건대맛집"}; 
        Map<String, JSONArray> bufferMap = new HashMap<>();
        bufferMap.put("여의도맛집", new JSONArray());
        bufferMap.put("노원맛집", new JSONArray());
        bufferMap.put("강남맛집", new JSONArray());
        bufferMap.put("용산맛집", new JSONArray());
        bufferMap.put("을지로맛집", new JSONArray());
        bufferMap.put("연남맛집", new JSONArray());
        bufferMap.put("잠실맛집", new JSONArray());
        bufferMap.put("건대맛집", new JSONArray());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            for (String jmt : JMT) {
                JSONArray buffer = bufferMap.get(jmt);
                if (buffer.length() > 0) {
                    sendRemainingDataToS3(buffer, jmt);
                }
            }
        }));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        
            for (ConsumerRecord<String, String> record : records) {

                    for (String jmt : JMT) {
                        System.out.println("for문 입성");
                        try{
                            String emojiParsedValue = EmojiParser.parseToAliases(record.value());
                            JSONObject final_jsonObject = new JSONObject(emojiParsedValue);
                            System.out.println("파싱성공");
                            int flag = 0;
                            for (String key : final_jsonObject.keySet()){
                                if (key.compareTo(jmt) == 0){
                                    flag = 1;
                                     JSONArray buffer = bufferMap.get(jmt);
                                     System.out.println("해당버퍼를 가져온다");
                                     
                                     buffer.put(final_jsonObject);
                                     System.out.println("버퍼에 추가 성공");
                                     sendToS3File(record, buffer, jmt);
                                }
                        }
                        if(flag ==1){break;}//찾았으니깐 break
                        }catch (JSONException e) {
                            System.out.println("error"+record.value());
                            e.printStackTrace();
                        }                           
                           
                        }
                
            }
            consumer.commitSync(); 
        }
       
    }


    public static void sendToS3File(ConsumerRecord<String, String> record, JSONArray buffer, String JMT){       
    
    Boolean check_fd = buffer.length() >= BUFFER_SIZE; 
  
    System.out.println("파일보냄"+record.value());
    System.out.println("버퍼 사이즈"+ JMT+buffer.length());

    if (check_fd) {
       
        String s3Key = "kafka-data/" + "instar_delete_ad.kafka" + "/"+ "new"+ "/"+ JMT + "/" +getCurrentDayOfWeek()+"/" + System.currentTimeMillis() + ".json";

        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType("application/json")
                .build();

        RequestBody requestBody = RequestBody.fromString(buffer.toString());
        
        s3Client.putObject(putRequest, requestBody);
        System.out.println("Uploaded data to S3: " + s3Key);

        buffer.clear();
        }
    
    }


    private static void sendRemainingDataToS3(JSONArray buffer, String JMT) {
        
        String s3Key = "kafka-data/" + "instar_delete_ad.kafka" + "/"+ "new"+ "/"+ JMT + "/" +getCurrentDayOfWeek()+"/" + System.currentTimeMillis() + ".json";

        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType("application/json")
                .build();

        RequestBody requestBody = RequestBody.fromString(buffer.toString());
        s3Client.putObject(putRequest, requestBody);
        System.out.println("Uploaded remaining data to S3: " + s3Key);
    }
    public static String getCurrentDayOfWeek() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant instant = timestamp.toInstant();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        DayOfWeek dayOfWeek = localDateTime.getDayOfWeek();
        return dayOfWeek.toString();
    }

}

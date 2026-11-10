package example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class ConsumerFunctions {

    public static KafkaConsumer<String, String> CreateConsumer(String bootstrapServers) {
        // 1. Настройка консюмера
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Читать с начала
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        return new KafkaConsumer<>(properties);
    }


    public static List<String> StartConsumingMessages(KafkaConsumer<String, String> consumer, int messagesCount)
    {
        List<String> receivedMessages = new java.util.ArrayList<>();
        try {
            // Потребление сообщений
            int messagesReceived = 0;
            System.out.println("");
            while (messagesReceived < messagesCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.printf("📨 Получено сообщение: ключ=%s, значение=%s, партиция=%d, оффсет=%d, временная метка=%d%n",
                            record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
                    messagesReceived++;
                    receivedMessages.add(record.value());
                }

                if (records.isEmpty()) {
                    System.out.print(".");
                }
            }

            System.out.println("\n-------------------------------------------");
            System.out.println("Получено " + messagesReceived + " сообщений.");

        } catch (Exception e) {
            e.printStackTrace();
        }

        return receivedMessages;
    }

    public static List<String> StartConsumingMessages(KafkaConsumer<String, String> consumer, int messagesCount, String key)
    {
        List<String> receivedMessages = new java.util.ArrayList<>();
        try {
            // Потребление сообщений
            int messagesReceived = 0;
            System.out.println("");
            while (messagesReceived < messagesCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.printf("📨 Получено сообщение: ключ=%s, значение=%s, партиция=%d, оффсет=%d, временная метка=%d%n",
                            record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
                    messagesReceived++;
                    if (record.key().equals(key))
                    {
                        receivedMessages.add(record.value());
                    }
                }

                if (records.isEmpty()) {
                    System.out.print(".");
                }
            }

            System.out.println("\n-------------------------------------------");
            System.out.println("Получено " + messagesReceived + " сообщений.");

        } catch (Exception e) {
            e.printStackTrace();
        }

        return receivedMessages;
    }


    public static void CloseConsumer(KafkaConsumer<String, String> consumer)
    {
        // Закрытие консюмера
        consumer.close();
        System.out.println("Консюмер закрыт.");
    }
}

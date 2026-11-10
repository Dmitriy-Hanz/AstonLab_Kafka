package example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class ProducerFunctions {

    public static KafkaProducer<String, String> CreateProducer(String bootstrapServers) {
        // 1. Настройка продюсера
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all"); // Подтверждение от всех реплик
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);// Уменьшаем таймаут
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);

        return new KafkaProducer<>(properties);
    }

    public static void StartSendingMessages(KafkaProducer<String, String> producer, int messagesCount, String keyPrefix)
    {
        Random random = new Random();

        try {
            // Отправка  тестовых сообщений
            for (int i = 1; i <= messagesCount; i++) {
                String key = keyPrefix + i + "_" + Math.round(random.nextDouble()*1000);
                String value = "Сообщение #" + keyPrefix + i + " отправлено в " + System.currentTimeMillis();

                ProducerRecord<String, String> record = new ProducerRecord<>(Cns.TEST_TOPIC_NAME, key, value);

                // Асинхронная отправка с колбэком
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("✅ Успешно отправлено: ключ=%s, значение=%s, " +
                                        "партиция=%d, оффсет=%d%n",
                                key, value,
                                metadata.partition(),
                                metadata.offset());
                    } else {
                        System.err.printf("❌ Ошибка отправки: %s%n", exception.getMessage());
                    }
                });

                Thread.sleep(1000); // Задержка для наглядности
            }

            producer.flush();
            System.out.println("Все сообщения отправлены!");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // восстанавливаем флаг прерывания
            e.printStackTrace();
        }
    }

    public static Future<RecordMetadata> SendMessage(KafkaProducer<String, String> producer, String message, String key) {
        Random random = new Random();

        ProducerRecord<String, String> record = new ProducerRecord<>(Cns.TEST_TOPIC_NAME, key, message);
        // Асинхронная отправка с колбэком
        Future<RecordMetadata> callback = producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("✅ Успешно отправлено: ключ=%s, значение=%s, " +
                                "партиция=%d, оффсет=%d%n",
                        key, message,
                        metadata.partition(),
                        metadata.offset());
            } else {
                System.err.printf("❌ Ошибка отправки: ключ= " + key + " | сообщение= "+message+" | ошибка: %s%n", exception.getMessage());
            }
        });

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return callback;
    }



    public static void CloseProducer(KafkaProducer<String, String> producer)
    {
        //Закрытие продюсера
        producer.flush();
        producer.close();
        System.out.println("Продюсер закрыт.");
    }

}

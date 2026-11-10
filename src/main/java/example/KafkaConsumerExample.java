package example;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Random;

public class KafkaConsumerExample {
    public static void main(String[] args) {
        try {
            System.setOut(new java.io.PrintStream(System.out, true, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        try {
            // Все ошибки и логи библиотек (System.err) пишем в файл
            System.setErr(new PrintStream(new FileOutputStream("logs/consumer-logs.log")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        Random random = new Random();

        // 2. Создание консюмера
        KafkaConsumer<String, String> consumer = ConsumerFunctions.CreateConsumer(Cns.BOOTSTRAP_SERVERS);

        TopicFunctions.createTopicIfExists(Cns.TEST_TOPIC_NAME,Cns.BOOTSTRAP_SERVERS, 3, 1);
        consumer.subscribe(List.of(Cns.TEST_TOPIC_NAME));

//        System.out.println("Bootstrap servers: " + properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Начинаем потребление сообщений из топика: " + Cns.TEST_TOPIC_NAME);
        System.out.println("Ожидание сообщений... (Ctrl+C для остановки)");
        System.out.println("-------------------------------------------");

        // Потребление сообщений
        ConsumerFunctions.StartConsumingMessages(consumer,10);

        //Закрытие консюмера
        ConsumerFunctions.CloseConsumer(consumer);
    }
}

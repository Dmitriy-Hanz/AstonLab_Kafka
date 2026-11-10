package example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaBrokerFailureTest {

    @BeforeAll
    static void setup() {
        try {
            System.setOut(new java.io.PrintStream(System.out, true, "UTF-8"));
            System.setOut(new java.io.PrintStream(System.err, true, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        TopicFunctions.deleteTopicIfExists(Cns.TEST_TOPIC_NAME, Cns.BOOTSTRAP_SERVERS);

        TopicFunctions.createTopicIfExists(Cns.TEST_TOPIC_NAME, Cns.BOOTSTRAP_SERVERS, 3,1);
    }

    @Test
    void shouldHandleBrokerFailure() throws InterruptedException, IOException, ExecutionException {

        String groupId = "failure-test-" + System.currentTimeMillis();
        int messagesBefore = 2;
        int messagesDuring = 1;
        int messagesAfter = 2;

        //Создание продюсера
        KafkaProducer<String, String> producer = ProducerFunctions.CreateProducer(Cns.BOOTSTRAP_SERVERS);
        //создание консюмера
        KafkaConsumer<String, String> consumer = ConsumerFunctions.CreateConsumer(Cns.BOOTSTRAP_SERVERS);
        //подписка консюмера на топик
        consumer.subscribe(List.of(Cns.TEST_TOPIC_NAME));

        // 1. Отправляем сообщения до отключения
        System.out.println("Начинаем отправку сообщений...");
        ProducerFunctions.StartSendingMessages(producer,messagesBefore, "before-");

        // 2. Останавливаем брокера
        System.out.println("⏳ Останавливаем все брокеры...");
        Process stop = new ProcessBuilder("docker-compose", "stop", "kafka-broker-1", "kafka-broker-2", "kafka-broker-3")
                .directory(new File("D:\\АСТОН\\задачное\\для кафки\\kafka-cluster"))
                .inheritIO().start();
        stop.waitFor();

        // 3. Отправляем сообщение во время отключения
        System.out.println("Пытаемся отправить сообщение во время отключения...");
        Future<RecordMetadata> callback = ProducerFunctions.SendMessage(producer, "during-message", "1");

        // 4. Запускаем брокера
        System.out.println("⏳ Запускаем брокеры...");
        Process start = new ProcessBuilder("docker-compose", "start", "kafka-broker-1", "kafka-broker-2", "kafka-broker-3")
                .directory(new File("D:\\АСТОН\\задачное\\для кафки\\kafka-cluster"))
                .inheritIO().start();
        start.waitFor();

        // 5. Отправляем после запуска
        System.out.println("Отправляем сообщения после запуска...");
        ProducerFunctions.StartSendingMessages(producer,messagesAfter, "after-");

        // 6. Читаем все сообщения (должны быть все 5)
        System.out.println("Начинаем потребление сообщений из топика: " + Cns.TEST_TOPIC_NAME);
        System.out.println("Ожидание сообщений... (Ctrl+C для остановки)");
        System.out.println("-------------------------------------------");
        List<String> received = ConsumerFunctions.StartConsumingMessages(consumer,messagesBefore + messagesAfter);

        // Проверяем, что у нас есть сообщения с разными префиксами
        long beforeCount = received.stream().filter(msg -> msg.contains("before")).count();
        long afterCount = received.stream().filter(msg -> msg.contains("after")).count();

        assertThat(beforeCount).isGreaterThanOrEqualTo(messagesBefore);
        assertThat(afterCount).isGreaterThanOrEqualTo(messagesAfter);
    }
}

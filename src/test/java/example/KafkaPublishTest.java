package example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class KafkaPublishTest {

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
    void shouldPublishAndConsumeMessages() throws ExecutionException, InterruptedException {
        int messageCount = 5;
        String groupId = "test-group-" + System.currentTimeMillis();

        //Создание продюсера
        KafkaProducer<String, String> producer = ProducerFunctions.CreateProducer(Cns.BOOTSTRAP_SERVERS);

        // Отправляем сообщения
        System.out.println("Начинаем отправку сообщений...");
        ProducerFunctions.StartSendingMessages(producer,messageCount, "");

        //создание консюмера
        KafkaConsumer<String, String> consumer = ConsumerFunctions.CreateConsumer(Cns.BOOTSTRAP_SERVERS);
        //подписка консюмера на топик
        consumer.subscribe(List.of(Cns.TEST_TOPIC_NAME));

        System.out.println("Начинаем потребление сообщений из топика: " + Cns.TEST_TOPIC_NAME);
        System.out.println("Ожидание сообщений... (Ctrl+C для остановки)");
        System.out.println("-------------------------------------------");
        // Читаем сообщения
        List<String> receivedMessages = ConsumerFunctions.StartConsumingMessages(consumer,messageCount);

        // 3. Проверяем
        assertThat(receivedMessages)
                .hasSize(messageCount)
                .allMatch(msg -> msg.startsWith("Сообщение #"));
    }


}

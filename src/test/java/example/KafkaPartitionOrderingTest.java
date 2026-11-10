package example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class KafkaPartitionOrderingTest {

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
    void shouldPreserveOrderWithinPartition() throws ExecutionException, InterruptedException {
        String groupId = "order-test-" + System.currentTimeMillis();
        int messageCount = 6;
        String sameKey = "same-key-123";

        //Создание продюсера
        KafkaProducer<String, String> producer = ProducerFunctions.CreateProducer(Cns.BOOTSTRAP_SERVERS);
        //создание консюмера
        KafkaConsumer<String, String> consumer = ConsumerFunctions.CreateConsumer(Cns.BOOTSTRAP_SERVERS);
        //подписка консюмера на топик
        consumer.subscribe(List.of(Cns.TEST_TOPIC_NAME));

        // 1. Отправляем сообщения с одинаковым ключом (они попадут в одну партицию)
        System.out.println("Начинаем отправку сообщений...");
        for (int i = 1; i <= messageCount; i++) {
            ProducerFunctions.SendMessage(producer,"Сообщение #" + i, sameKey);
        }

        // 2. Читаем сообщения с этим ключом
        System.out.println("Начинаем потребление сообщений из топика: " + Cns.TEST_TOPIC_NAME);
        System.out.println("Ожидание сообщений... (Ctrl+C для остановки)");
        System.out.println("-------------------------------------------");
        List<String> received = ConsumerFunctions.StartConsumingMessages(consumer,messageCount, sameKey);

        // 3. Проверяем порядок
        assertThat(received)
                .hasSize(messageCount)
                .containsExactly(
                        "Сообщение #1", "Сообщение #2", "Сообщение #3",
                        "Сообщение #4", "Сообщение #5", "Сообщение #6"
                );
    }

}

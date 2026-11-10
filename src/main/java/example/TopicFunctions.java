package example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TopicFunctions {
    public static void createTopicIfExists(String topicName, String bootstrapServers, int partitions, int replicationFactor) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, (short)replicationFactor);
            admin.createTopics(Collections.singleton(newTopic))
                    .all()
                    .get();
            System.out.println("✅ Топик готов: " + topicName);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.println("ℹ️ Топик уже существует: " + topicName);
            } else {
                e.printStackTrace();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void deleteTopicIfExists(String topicName, String bootstrapServers)
    {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // ГАРАНТИЯ ПРОТИВ ТАЙМАУТА: Увеличиваем внутреннее время ожидания API клиента
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");     // 15 секунд
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000"); // 15 секунд

        try (AdminClient admin = AdminClient.create(props)) {

            // 1. Создаем опции с запасом времени на холодный старт
            DescribeTopicsOptions options = new DescribeTopicsOptions().timeoutMs(10000);

            // 2. Запрашиваем описание топика
            DescribeTopicsResult result = admin.describeTopics(
                    Collections.singletonList(topicName),
                    options
            );

            try {
                // 2. Ожидаем результат. Если топика нет, выбросится ExecutionException
                result.allTopicNames().get(10, TimeUnit.SECONDS);

                // Если код прошел сюда, топик существует — удаляем его
                DeleteTopicsResult deleteResult = admin.deleteTopics(Collections.singleton(topicName));
                deleteResult.all().get(10, TimeUnit.SECONDS);
                System.out.println("✅ Топик удалён");

            }
            catch (ExecutionException e) {
                // Проверяем, вызвано ли исключение тем, что топика просто не существует
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    System.out.println("ℹ️ Топик не существовал");
                } else {
                    System.err.println("❌ Ошибка при проверке/удалении топика: " + e.getMessage());
                }
            }
            catch (InterruptedException e) {
                System.err.println("❌ Поток был прерван");
                Thread.currentThread().interrupt();
            }
            catch (TimeoutException e) {
                System.err.println("❌ Превышено время ожидания ответа от Kafka");
            }

        }
    }
}

package example;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.net.InetSocketAddress;
import java.net.Socket;
import static org.assertj.core.api.Assertions.assertThat;

public class DockerConnectionTest {

    @Test
    void checkConnectionTest() {
        String host = "localhost", port = "9092", address = host + ":" + port;
        System.out.printf("🔍 Проверяем доступность %s... ", address);

        // Пытаемся открыть чистое TCP-соединение с таймаутом в 2 секунды
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, Integer.parseInt(port)), 2000);
            System.out.println("✅ Успешно! Порт Docker открыт и отвечает.");
            assertThat(true).isTrue();
        } catch (IOException e) {
            System.err.println("❌ Ошибка подключения!");
            System.err.println("   Причина: " + e.getMessage());
            System.err.println("   Что это значит: Ваше Java-приложение физически не может достучаться до Docker.");
            assertThat(false).isTrue();
        }
    }
}

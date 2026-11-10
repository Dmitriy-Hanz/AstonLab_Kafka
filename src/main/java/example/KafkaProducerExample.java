package example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class KafkaProducerExample {
    public static void main(String[] args) {
        try {
            System.setOut(new java.io.PrintStream(System.out, true, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        try {
            // Все ошибки и логи библиотек (System.err) пишем в файл
            System.setErr(new PrintStream(new FileOutputStream("logs/producer-logs.log")));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 2. Создание продюсера
        KafkaProducer<String, String> producer = ProducerFunctions.CreateProducer(Cns.BOOTSTRAP_SERVERS);


        System.out.println("Начинаем отправку сообщений...");
        // Отправка 10 сообщений
        ProducerFunctions.StartSendingMessages(producer,10,"");

        //закрытие продюсера
        ProducerFunctions.CloseProducer(producer);
    }
}

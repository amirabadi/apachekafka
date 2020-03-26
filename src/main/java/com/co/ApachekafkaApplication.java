package com.co;

import com.co.config.Greeting;
import com.co.config.MessageListener;
import com.co.config.MessageProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class ApachekafkaApplication {

    public static void main(String[] args) throws InterruptedException {

        ConfigurableApplicationContext context = SpringApplication.run(ApachekafkaApplication.class, args);

        MessageProducer.MessagesProducer producer = context.getBean(MessageProducer.MessagesProducer.class);
        MessageListener.MessagesListener listener = context.getBean(MessageListener.MessagesListener.class);
        /*
         * Sending a Hello World message to topic 'baeldung'.
         * Must be recieved by both listeners with group foo
         * and bar with containerFactory fooKafkaListenerContainerFactory
         * and barKafkaListenerContainerFactory respectively.
         * It will also be recieved by the listener with
         * headersKafkaListenerContainerFactory as container factory
         */
        producer.sendMessage("Hello, World!");
        listener.getLatch().await(10, TimeUnit.SECONDS);

        /*
         * Sending message to a topic with 5 partition,
         * each message to a different partition. But as per
         * listener configuration, only the messages from
         * partition 0 and 3 will be consumed.
         */
        for (int i = 0; i < 5; i++) {
            producer.sendMessageToPartion("Hello To Partioned Topic!", i);
        }
        listener.getPartitionLatch().await(10, TimeUnit.SECONDS);

        /*
         * Sending message to 'filtered' topic. As per listener
         * configuration,  all messages with char sequence
         * 'World' will be discarded.
         */
        producer.sendMessageToFiltered("Hello Baeldung!");
        producer.sendMessageToFiltered("Hello World!");
        listener.getFilterLatch().await(10, TimeUnit.SECONDS);

        /*
         * Sending message to 'greeting' topic. This will send
         * and recieved a java object with the help of
         * greetingKafkaListenerContainerFactory.
         */
        producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
        listener.getGreetingLatch().await(10, TimeUnit.SECONDS);

        context.close();
        /* SpringApplication.run(ApachekafkaApplication.class, args); */
    }
    @Bean
    public MessageProducer.MessagesProducer messageProducer() {
        return new MessageProducer.MessagesProducer();
    }

    @Bean
    public MessageListener.MessagesListener messageListener() {
        return new MessageListener.MessagesListener();
    }
}

package kafka.consumer;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

import germanoi.EventBuffer;
import com.fasterxml.jackson.databind.ObjectMapper;
import events.ABCEvent;
import events.Source;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import managers.EventManager;

public class CustomKafkaListener<T> implements Runnable {
    private static final Logger log = Logger.getLogger(CustomKafkaListener.class.getName());
    private String topic;
    private KafkaConsumer<String, String> consumer;
    private Consumer<String> recordConsumer;
    private Set<T> messageSet;
    private ObjectMapper objectMapper;
    private EventManager eventManager;
    private Source<T> source;
    private EventBuffer buffer;


    public CustomKafkaListener(String topic, KafkaConsumer<String, String> consumer, Set<T> tree, Source<T> source) {
        this.topic = topic;
        this.consumer = consumer;
        this.recordConsumer = record -> log.info("received: " + record);
        this.messageSet = tree;
        this.objectMapper = new ObjectMapper();
        this.eventManager = null;
        this.source = source;
    }

    public CustomKafkaListener(String topic, KafkaConsumer<String, String> consumer, Set<T> tree, EventManager eventManager, Source<T> source, EventBuffer buffer) {
        this.topic = topic;
        this.consumer = consumer;
        this.recordConsumer = record -> log.info("received: " + record);
        this.messageSet = tree;
        this.objectMapper = new ObjectMapper();
        this.eventManager = eventManager;
        this.source = source;
        this.buffer = buffer;
    }

    public CustomKafkaListener(String topic, String bootstrapServers, Set<T> tree, Source<T> source) {
        this(topic, defaultKafkaConsumer(bootstrapServers), tree, source);
    }

    public CustomKafkaListener(String topic, String bootstrapServers, Set<T> tree, EventManager eventManager, Source<T> source, EventBuffer buffer) {
        this(topic, defaultKafkaConsumer(bootstrapServers), tree, eventManager, source, buffer);
    }

    private static KafkaConsumer<String, String> defaultKafkaConsumer(String boostrapServers) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        return new KafkaConsumer<>(props);
    }

    public void setEventManager(EventManager eventManager) {
        this.eventManager = eventManager;
    }

    public CustomKafkaListener onEach(Consumer<String> newConsumer) {
        recordConsumer = recordConsumer.andThen(newConsumer);
        return this;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (!Thread.currentThread().isInterrupted()) {
                consumer.poll(Duration.ofMillis(100))
                    .forEach(this::processRecord);
            }
        } catch (WakeupException e) {
            // Ignore for shutdown
        } finally {
            consumer.close(); // Ensure the consumer is properly closed
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {

        try {
            if (eventManager != null) {
                //            System.out.println(record.value());
                JSONObject message = new JSONObject(record.value());
                //            System.out.println(message);
                //            System.out.println(record.timestamp());
                //                this.messageSet.add((T) event);
                ArrayList<ABCEvent> eventsExtracted = source.processMessage(message);
                for (ABCEvent e : eventsExtracted) {
                    System.out.println("Source == " + source.name() + "-- event ts == " + e.getTimestampDate());
                    eventManager.acceptEvent(e.getType(), e);
                    buffer.addEvent(e);
                }
                consumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)));
            }
        }catch (Exception e){
            System.out.println("Failed to process record: "+e.getMessage());
        }
    }

    public void shutdown() {
        consumer.wakeup(); // Used to break out of the poll loop
    }

}

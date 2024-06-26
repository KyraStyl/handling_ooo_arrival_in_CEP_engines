package kafka.consumer;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

import germanoi.EventBuffer;
import com.fasterxml.jackson.databind.ObjectMapper;
import events.ABCEvent;
import events.Source;
import germanoi.SpeculativeProcessor;
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
    private SpeculativeProcessor speculativeProcessor;


    public CustomKafkaListener(String topic, KafkaConsumer<String, String> consumer, Set<T> tree, Source<T> source) {
        this.topic = topic;
        this.consumer = consumer;
        this.recordConsumer = record -> log.info("received: " + record);
        this.messageSet = tree;
        this.objectMapper = new ObjectMapper();
        this.eventManager = null;
        this.source = source;
    }

    public CustomKafkaListener(String topic, KafkaConsumer<String, String> consumer, Set<T> tree, EventManager eventManager, Source<T> source, SpeculativeProcessor speculativeProcessor) {
        this.topic = topic;
        this.consumer = consumer;
        this.recordConsumer = record -> log.info("received: " + record);
        this.messageSet = tree;
        this.objectMapper = new ObjectMapper();
        this.eventManager = eventManager;
        this.source = source;
        this.speculativeProcessor = speculativeProcessor;
    }

    public CustomKafkaListener(String topic, String bootstrapServers, Set<T> tree, Source<T> source) {
        this(topic, defaultKafkaConsumer(bootstrapServers), tree, source);
    }

    public CustomKafkaListener(String topic, String bootstrapServers, Set tree, EventManager eventManager, Source source, SpeculativeProcessor speculativeProcessor) {
        this(topic, defaultKafkaConsumer(bootstrapServers), tree, eventManager, source, speculativeProcessor);
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
            if (eventManager != null || speculativeProcessor != null) {
                JSONObject message = new JSONObject(record.value());
                ArrayList<ABCEvent> eventsExtracted = source.processMessage(message);
                boolean terminate = false;
                for (ABCEvent e : eventsExtracted) {
                    System.out.println("Source == " + source.name() + "-- event ts == " + e.getTimestampDate());
                    terminate = source.name().equalsIgnoreCase("terminate");
                    if(eventManager != null)
                        eventManager.acceptEvent(e.getType(), e);
                    if(speculativeProcessor != null)
                        speculativeProcessor.processEvent(e);
                }
                consumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)));
//                if(terminate)
//                    System.exit(200);

            }
        }catch (Exception e){
            System.out.println("Failed to process record: "+e.getMessage());
        }
    }

    public void shutdown() {
        consumer.wakeup(); // Used to break out of the poll loop
    }

}

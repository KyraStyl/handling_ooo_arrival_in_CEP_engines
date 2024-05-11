import germanoi.EventBuffer;
import events.*;
import examples.ExampleCEP;
import examples.LCExample;
import germanoi.SpeculativeProcessor;
import kafka.*;
import kafka.consumer.ConsumeInRangeMultipleTopics;
import kafka.consumer.CustomKafkaListener;
import managers.EventManager;
import utils.ApplicationConstant;
import utils.Configs;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static utils.ApplicationConstant.*;


public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

//        try {
//            InputParamParser.validateParams(args);
//            InputParamParser.readParams();
//        }catch (Exception e){
//            System.out.println(InputParamParser.getHelp());
//            System.exit(100);
//        }

        boolean runMine = true;

        String defaultBootStrapServer = KAFKA_LOCAL_SERVER_CONFIG;
        KafkaAdminClient kafkaAdminClient = new KafkaAdminClient(defaultBootStrapServer);
        System.out.println(kafkaAdminClient.verifyConnection());

        ArrayList<String> listOfSources = new ArrayList<>();

        listOfSources.add("fitbit");
        listOfSources.add("locations");
        listOfSources.add("scale");

        ExampleCEP lc = new LCExample();
        lc.initializeExample();

        ArrayList<Source> sources = lc.getSources();
        HashMap<String,Long> estimatedArrivalTime = lc.getEstimated();

        Configs globalConfigs = new Configs();

        EventManager<ABCEvent> eventManager = null;
        SpeculativeProcessor speculativeProcessor = null;

        if(runMine){
            eventManager = new EventManager<>("src/main/resources/test.query", sources, estimatedArrivalTime, globalConfigs);
            eventManager.initializeManager();
        }else{
            speculativeProcessor = new SpeculativeProcessor(globalConfigs);
            EventBuffer buffer = new EventBuffer(globalConfigs);
        }

        HashMap<String, Set<ABCEvent>> hashlist = new HashMap<>();
        HashMap<String, CustomKafkaListener> consumers = new HashMap<>();
        HashMap<String, Thread> threadsConsumers = new HashMap<>();

//        List<String> topics = new ArrayList<>();
//        topics.add("fitbit");
//        topics.add("locations");
//        ABCEvent mpw_start = new ABCEvent();
//        ConsumeInRangeMultipleTopics kfConsumer = new ConsumeInRangeMultipleTopics(topics, ApplicationConstant.KAFKA_LOCAL_SERVER_CONFIG, this, mpw_start.getTimestamp().getTime(), mpw_end.getTimestamp().getTime());

        for(Source source:sources){
            Set<ABCEvent> tree = new TreeSet<>(new TimestampComparator());
            hashlist.put(source.name(), tree);
            consumers.put(source.name(), new CustomKafkaListener(source.name(), defaultBootStrapServer, hashlist.get(source), eventManager, source, speculativeProcessor));
            threadsConsumers.put(source.name(), new Thread(consumers.get(source.name())));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumers.values().forEach(CustomKafkaListener::shutdown);
            threadsConsumers.values().forEach(thread -> {
                try {
                    thread.join();  // Wait for all threads to finish
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Failed to stop consumer threads gracefully");
                }
            });
        }));

        threadsConsumers.values().forEach(Thread::start);

    }
}
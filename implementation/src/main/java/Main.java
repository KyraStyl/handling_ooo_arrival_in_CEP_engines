import events.*;
import examples.ExampleCEP;
import examples.LCExample;
import kafka.*;
import kafka.consumer.CustomKafkaListener;
import managers.EventManager;

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


        EventManager<ABCEvent> eventManager = new EventManager<>("src/main/resources/test.query", sources, estimatedArrivalTime);
        eventManager.initializeManager();

//        ABCEvent event1 = new KeyValueEvent<Integer>("F1","2024-01-02T14:00:00.200", "Fitbit", "Steps","steps",120);
//        ABCEvent event2 = new KeyValueEvent<Integer>("F2","2024-01-02T14:05:00.200", "Fitbit", "Stairs","stairs",3);
//        ABCEvent event3 = new KeyValueEvent<Double>("S1","2024-01-02T14:20:00.200", "Scale", "Weight","weight",68.3);
//        ABCEvent event4 = new KeyValueEvent<Double>("S2","2024-01-02T14:03:00.200", "Scale", "Height","height",165.2);


//        TreeSet<ABCEvent> test = new TreeSet<>(new TimestampComparator());
//        test.add(event1);
//        test.add(event2);
//        System.out.println(test.lower(event3));
//        System.out.println(test.headSet(event3));
//        System.out.println(test.lower(event4));
//        System.out.println(test.headSet(event4));

        HashMap<String, Set<ABCEvent>> hashlist = new HashMap<>();
        HashMap<String, CustomKafkaListener> consumers = new HashMap<>();
        HashMap<String, Thread> threadsConsumers = new HashMap<>();

        for(Source source:sources){
            Set<ABCEvent> tree = new TreeSet<>(new TimestampComparator());
            hashlist.put(source.name(), tree);
            consumers.put(source.name(), new CustomKafkaListener(source.name(), defaultBootStrapServer, hashlist.get(source), eventManager, source));
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


//        Date ts1 = castStrToDate("2024-02-03T10:45:50.020");
//        Date ts2 = castStrToDate("2024-02-01T10:45:50.020");
//        Date ts3 = castStrToDate("2024-02-03T10:40:50.020");
//
//
//        // Elements are added using add() method
//        tree.add(new ABCEvent("A","2024-02-03T10:45:50.020")); {"name":"A","timestamp":"2024-02-03T10:45:50.020"}
//        tree.add(new ABCEvent("B","2024-02-01T10:45:50.020")); {"name":"B","timestamp":"2024-02-01T10:45:50.020"}
//        tree.add(new ABCEvent("C","2024-02-03T10:40:50.020")); {"name":"C","timestamp":"2024-02-03T10:40:50.020"}
//
//        // Duplicates will not get insert
//        tree.add(new ABCEvent("C",ts3));
//
//        // Elements get stored in default natural
//        // Sorting Order(Ascending)
//        System.out.println(tree);
    }
}
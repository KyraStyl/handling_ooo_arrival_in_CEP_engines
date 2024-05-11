//package germanoi;
//
//import events.ABCEvent;
//
//import java.util.Date;
//
//public class EventSystem {
//    public static void main(String[] args) {
//        SpeculativeProcessor processor = new SpeculativeProcessor();
//
//        // Simulate a stream of events
//        for (int i = 0; i < 100; i++) {
//            ABCEvent event = new ABCEvent("evt_"+i,new Date(System.currentTimeMillis()).toString() ,"type" + (i % 5),i);
//            processor.processEvent(event);
//            processor.adaptSpeculation(); // Adjust speculation rate periodically
//
//            try {
//                Thread.sleep(100); // Simulating time delay between events
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}
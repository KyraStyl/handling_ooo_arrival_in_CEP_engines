package managers;

import kafka.consumer.ConsumeInRangeMultipleTopics;
import stats.Profiling;
import stats.StatisticManager;
import cep.sasesystem.engine.EngineController;
import cep.sasesystem.query.State;
import cep.sasesystem.query.Transition;
import events.ABCEvent;
import events.Source;
import events.TimestampComparator;
import net.sourceforge.jeval.EvaluationException;
import utils.ApplicationConstant;
import utils.Configs;

import java.util.*;
import java.util.stream.Collectors;

import static utils.UsefulFunctions.capitalize;

public class EventManager<T> {

    private HashMap<String, ArrayList<ABCEvent>> allEventsReceived;
    HashMap<String, TreeSet<ABCEvent>> acceptedEventsHashlist = new HashMap<>();
    private EngineController engineController;
    private StatisticManager statisticManager;
    private ResultManager resultManager;
    private Configs configs;
    private ArrayList<Source> sources;
    private Date latest_ts_arrived;
    private Date oldest_ts_arrived;
    private Profiling profiling;


    public EventManager (ArrayList<Source> sources, Configs configs){
        this.allEventsReceived = new HashMap<>();
        this.engineController = new EngineController("once");
        this.statisticManager = new StatisticManager();
        this.profiling = new Profiling("Our Solution");
        this.resultManager = new ResultManager(this.profiling);
        this.configs = configs;
        this.sources = sources;
    }

    public EventManager (ArrayList<Source> sources, HashMap<String, Long> estimated, Configs configs){
        this(sources,configs);
        this.statisticManager.setEstimated(estimated);
    }

    public void initializeManager(){
        this.latest_ts_arrived = null;
        this.oldest_ts_arrived = null;
        this.allEventsReceived = new HashMap<>();
        this.engineController.setConfigs(this.configs);
        this.engineController.setNfa(this.configs.nfaFileLocation());
        this.engineController.setEngine();
        this.engineController.initializeEngine();
        initializeConfigs();
        this.engineController.setEngineResultManager(resultManager);
        this.statisticManager.initializeManager(this.configs.listOfStateTypes());
        this.initializeTreesets();
    }

    private void initializeConfigs(){
        this.configs.setStates(this.engineController.getStates());
        this.configs.setWindowLength(this.engineController.getWindow());
    }


    private void initializeTreesets(){
        for (State s : this.configs.states()){
            acceptedEventsHashlist.put(s.getEventType(),new TreeSet<ABCEvent>(new TimestampComparator()));
        }

    }

    public void acceptEvent(String source, T event){
        source = capitalize(source);

        if (terminate(event)){
            this.profiling.printProfiling();
//            return;
        }
        this.oldest_ts_arrived = this.oldest_ts_arrived == null? ((ABCEvent) event).getTimestampDate() : this.oldest_ts_arrived;

        this.profiling.increaseEvents();

        double oooscore = 0;

        if(this.configs.listOfStateTypes().contains(source)) {
            oooscore = processEvent((ABCEvent) event,source,find_last(event,source), (long) (configs.windowLength()));

            if(!this.allEventsReceived.containsKey(source))
                this.allEventsReceived.put(source, new ArrayList<>());
            this.allEventsReceived.get(source).add((ABCEvent) event);
            this.latest_ts_arrived = this.latest_ts_arrived == null?
                    ((ABCEvent) event).getTimestampDate() :
                    ((ABCEvent) event).getTimestampDate().getTime() > latest_ts_arrived.getTime() ?
                            ((ABCEvent) event).getTimestampDate() : latest_ts_arrived;
        }
        else return;

        if(oooscore == -2){
            System.out.println("SOMETHING IS WRONG WITH THE SOURCE");
            return;
        }else if (oooscore == -1){
            System.out.println("SOMETHING WENT WRONG!");
            return;
        }

        //this is an IN-ORDER event
        if (oooscore == 0){
            System.out.println("THIS IS AN IN-ORDER EVENT "+event);
           acceptedEventsHashlist.get(((ABCEvent) event).getType()).add((ABCEvent) event);
           if (this.configs.last_state().equals(((ABCEvent) event).getType())) {
               try {
                   engineController.runEngine((ABCEvent) event, (EventManager<ABCEvent>) this);
                   remove_expired_events((ABCEvent) event);
               } catch (CloneNotSupportedException e) {
                   throw new RuntimeException(e);
               } catch (EvaluationException e) {
                   throw new RuntimeException(e);
               }
           }
        }
        //this is an OUT-OF-ORDER event, but worth processing
        else if (oooscore <= statisticManager.calculateThreshold(source)) {
            System.out.println("THIS IS AN OUT-OF-ORDER EVENT "+event);
            manageOutOfOrderEvent((ABCEvent) event, source);
            //create a custom kafka consumer, retrieve data within a specific time range, and check matches affected
        }
        //this is just a TOO MUCH OUT-OF-ORDER.
        else{
            System.out.println("=====");
            System.out.println("An out-of-order event with very high score just arrived!");
            System.out.println(event.toString());
            System.out.println(oooscore);
            System.out.println("=====");
        }
    }

    private boolean terminate(T event) {
        return ((ABCEvent) event).getName().equalsIgnoreCase("terminate");
    }

    private ABCEvent find_last(T event, String source) {
        TreeSet<ABCEvent> treeset = acceptedEventsHashlist.get(source);
        if (treeset != null && !treeset.isEmpty())
            return treeset.last();
        else
            return (ABCEvent) event;
    }

    public HashMap<String, ArrayList<ABCEvent>> getAllEventsReceived() {
        return allEventsReceived;
    }

    public TreeSet<ABCEvent> getTreeset(String source){
        return acceptedEventsHashlist.get(source);
    }

    //the whole pipeline for processing an incoming event
    public double processEvent(ABCEvent e, String source, ABCEvent last, Long timeWindow) {
        double timediff = Math.abs(e.getTimestampDate().getTime() - last.getTimestampDate().getTime());;
        if (e.getTimestampDate().getTime() >= last.getTimestampDate().getTime() && ( (latest_ts_arrived!= null && e.getTimestampDate().getTime() >= latest_ts_arrived.getTime()) || latest_ts_arrived == null) ){ // this event is in-order
            statisticManager.processUpdateStats(e,0,timediff,source, false);
//            System.out.println("THIS IS AN IN-ORDER EVENT "+e);
            return 0;
        }else{
            if (timediff > statisticManager.estimatedArrivalRate.get(source)){
                if(timediff > statisticManager.actualArrivalRate.get(source)){
                    // this is an out-of-order event for sure
                    // calculate the score
                    // (keep track of the avg ooo per source, and check if current score is above 2*avg + x ? )
                    double score = statisticManager.calculateScore(e,source,last, timeWindow);
                    statisticManager.processUpdateStats(e, score, timediff, source, true);
                    return score;
                }else{
                    // maybe something is wrong with this source?
                    return -2;
                }
            }
        }
        return -1;
    }

    private void manageOutOfOrderEvent(ABCEvent e, String source) {
        String type = e.getType();

        if(!type.equals(configs.last_state()) && (!getTreeset(configs.last_state()).isEmpty() && e.compareTo(getTreeset(configs.last_state()).last()) >= 0)){
            System.out.println("NOT LAST STATE - AND - ARRIVED WITH TS AFTER LAST C");
            //ok, just add to the treeset
            acceptedEventsHashlist.get(source).add(e);
        }else{
            //arrived before last end_type or is of end_type => => need to re-compute results
            System.out.println("EITHER LAST STATE - OR - ARRIVED WITH TS BEFORE LAST C");

            //define maximum potential window (MPW)
            Date ts_start = new Date(e.getTimestampDate().getTime() - configs.windowLength());
            if(oldest_ts_arrived.getTime() > ts_start.getTime())
                ts_start = oldest_ts_arrived;

            ABCEvent mpw_start = new events.ABCEvent(e.getName()+"_temp", ts_start,e.getSource()+"_temp", e.getType(), e.getSymbol());

            Date ts_end = new Date(e.getTimestampDate().getTime() + configs.windowLength());
            if(latest_ts_arrived.getTime() < ts_end.getTime() )
                ts_end = latest_ts_arrived;

            ABCEvent mpw_end = new events.ABCEvent(e.getName()+"_temp", ts_end,e.getSource()+"_temp", e.getType(), e.getSymbol());;

            if (e.getType().equals(configs.first_state())){
                mpw_start = e;
            } else if (e.getType().equals(configs.last_state())) {
                mpw_end = e;
            }

            System.out.println("latest ts arrived == "+latest_ts_arrived+"  -- event ts == "+e.getTimestampDate()+ "  -- mpw_end ts == "+mpw_end.getTimestampDate());

            //find subsets
            HashMap<String, Object> results = calculate_subsets(mpw_start, mpw_end);

            HashMap<String, Boolean> booleans = (HashMap<String, Boolean>)(results.get("booleans"));
            HashMap<String, TreeSet<ABCEvent>> treesets = (HashMap<String, TreeSet<ABCEvent>>)(results.get("subsets"));

            //check if you have all events needed
            Boolean flag = false;
            for(String ss : booleans.keySet()){
                if (booleans.get(ss)){
                    flag = true;
                    break;
                }
            }

            flag = false;

            //if you dont have the appropriate data, create a custom on-demand kafka consumer, and retrieve the desired events
            if (flag){
                System.out.println("I DONT HAVE THE APPROPRIATE EVENTS");
                List<String> topics = new ArrayList<>();
                for(Source src: sources){
                    for(String s: booleans.keySet()) {
                        if (src.hasEventType(s))
                            topics.add(src.name());
                    }
                }

                topics = topics.stream().distinct().collect(Collectors.toList());
                ConsumeInRangeMultipleTopics kfConsumer = new ConsumeInRangeMultipleTopics(topics, ApplicationConstant.KAFKA_LOCAL_SERVER_CONFIG, this, mpw_start.getTimestampDate().getTime(), mpw_end.getTimestampDate().getTime());
                kfConsumer.setTreesets(treesets);

                Thread t = new Thread(kfConsumer);
                t.start();
                try{
                    t.join();
                }catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }

            }else{
                //trigger find_matches_once (function from engine)
                engineController.runOnDemand(treesets,e);
            }
        }
    }


    private HashMap<String, Object> calculate_subsets(events.ABCEvent start, events.ABCEvent end){
        System.out.println("CALCULATING SUBSETS");
        HashMap<String, Object> results = new HashMap<>();
        results.put("booleans", new HashMap<>());
        results.put("subsets", new HashMap<>());

        HashMap<String, Boolean> booleans = (HashMap<String, Boolean>)(results.get("booleans"));
        HashMap<String, TreeSet<ABCEvent>> treesets = (HashMap<String, TreeSet<ABCEvent>>)(results.get("subsets"));

        for( String key : configs.transitions().keySet()){
            Transition t = engineController.getTransitions().get(key);
            String type = t.getEventType();
            TreeSet<events.ABCEvent> set = getTreeset(type);

            boolean count = set.subSet(start,true,end,true).size() > 0;

            booleans.put(type,false);
            if(set == null || set.isEmpty() || !count ||
                    start.getTimestampDate().getTime() < set.first().getTimestampDate().getTime()
                    || end.getTimestampDate().getTime() > set.last().getTimestampDate().getTime())
                booleans.put(type,true);

            set = (TreeSet<events.ABCEvent>) set.subSet(start,true,end,true);

            treesets.put(type+"_subset",set);
        }

        return results;
    }

    private void remove_expired_events(events.ABCEvent end_event){

        ABCEvent exp_last_ = new events.ABCEvent(end_event.getName()+"_temp", new Date(end_event.getTimestampDate().getTime() - 2*configs.windowLength()),end_event.getSource()+"_temp", end_event.getType(), end_event.getSymbol());

        for( String key : configs.transitions().keySet()){
            Transition t = engineController.getTransitions().get(key);
            String type = t.getEventType();
            TreeSet<events.ABCEvent> set = getTreeset(type);

            if (set!=null && !set.isEmpty() && exp_last_.compareTo(set.first()) >= 0)
                set.headSet(exp_last_).clear();

        }
    }

    public void accept_onDemand(HashMap<String, TreeSet<ABCEvent>> treeSetHashMap){
        for(String key: treeSetHashMap.keySet()){
            if (treeSetHashMap.get(key).isEmpty())
                return;
        }
        engineController.runOnDemand(treeSetHashMap,treeSetHashMap.get(configs.last_state()).last());
    }

}

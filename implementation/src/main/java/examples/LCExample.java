package examples;

import events.*;
import handlers.FitbitMessageHandler;
import handlers.LocationMessageHandler;
import handlers.ScaleMessageHandler;
import handlers.TerminateMessageHandler;

import java.util.ArrayList;
import java.util.HashMap;

import static utils.UsefulFunctions.*;

public class LCExample implements ExampleCEP {

    private ArrayList<Source> sources;
    private HashMap<String, Long> estimated;


    @Override
    public void initializeExample() {
        initializeSources();
        initializeEstimated();
    }

    private void initializeSources() {
        this.sources = new ArrayList<>();
        Source fitbit = new Source("Fitbit", new FitbitMessageHandler(), secondsToMillis(10));
        fitbit.addType("Steps", KeyValueEvent.class);
        fitbit.addType("Stairs", KeyValueEvent.class);
        fitbit.addType("HR", KeyValueEvent.class);

        this.sources.add(fitbit);

        Source locations = new Source("Locations", new LocationMessageHandler(), secondsToMillis(30));
        locations.addType("Bedroom", Location.class);
//        locations.addType("Bathroom", KeyValueEvent.class);
//        locations.addType("Livingroom", KeyValueEvent.class);
//        locations.addType("Aggregated", KeyValueEvent.class);
//        locations.addType("AggregatedNight", KeyValueEvent.class);

        sources.add(locations);

        Source scale = new Source("Scale", new ScaleMessageHandler(), secondsToMillis(60));
        scale.addType("Weight", KeyValueEvent.class);
        scale.addType("Height", KeyValueEvent.class);
        scale.addType("BMI", KeyValueEvent.class);
        scale.addType("BMICategory", KeyValueEvent.class);

        sources.add(scale);

        Source terminate = new Source("Terminate", new TerminateMessageHandler(), secondsToMillis(10000000));
        sources.add(terminate);
    }

    private void initializeEstimated(){
        this.estimated = new HashMap<>();
        for(Source s: this.sources){
            for(Object type: s.getEventTypes()){
                estimated.put((String)type,s.estimated());
            }
        }
    }

    public ArrayList<Source> getSources() {
        return this.sources;
    }

    public HashMap<String,Long> getEstimated(){
        return this.estimated;
    }
}

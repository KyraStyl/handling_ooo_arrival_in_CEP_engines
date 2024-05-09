package handlers;

import events.ABCEvent;
import events.KeyValueEvent;
import org.json.JSONObject;

import java.util.ArrayList;

public class FitbitMessageHandler implements KafkaMessageHandler {

    private int counter = 0;
    private String source = "Fitbit";

    @Override
    public ArrayList<ABCEvent> processMessage(JSONObject message) {
        return extract_values(message);
    }

    private ArrayList<ABCEvent> extract_values(JSONObject input) {
        JSONObject fitbit = input.getJSONObject("fitbit");
        String date = fitbit.getString("timestamp");
        System.out.println("Date in handler == "+ date);
        double steps = fitbit.getDouble("steps");
        double stairs = fitbit.getDouble("stairs");
        double naps = fitbit.getDouble("naps");

        KeyValueEvent<Double> stepsE = new KeyValueEvent<>("steps_"+counter,date, this.source, "Steps" ,"steps", steps);
        KeyValueEvent<Double> stairsE = new KeyValueEvent<>("stairs_"+counter,date, this.source, "Stairs", "stairs", stairs);
        KeyValueEvent<Double> napsE = new KeyValueEvent<>("naps_"+counter,date, this.source, "Naps", "naps", naps);

        ArrayList<ABCEvent> toreturn = new ArrayList<>();
        toreturn.add(stepsE);
        toreturn.add(stairsE);
        toreturn.add(napsE);

        counter++;

        return toreturn;
    }


}

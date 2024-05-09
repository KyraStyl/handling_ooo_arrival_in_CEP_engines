package handlers;


import events.ABCEvent;
import events.Location;
import org.json.JSONObject;

import java.util.ArrayList;

public class LocationMessageHandler implements KafkaMessageHandler {

    private int counter = 0;

    @Override
    public ArrayList<ABCEvent> processMessage(JSONObject input) {
//        System.out.println(input);

        long patientID = 10;

        String loc = input.getString("location");
        String ts = input.getString("timestamp");
        System.out.println("Date in handler == "+ ts);
        String de = input.getString("dateEntered");
        String du = input.getString("dateUpdate");
        int ta = input.getInt("totalActivations");
        int ra = input.getInt("recentActivations");

        Location l = new Location("location_"+counter, ts, patientID, new Location.Position(loc,de, du, ta, ra));

        ArrayList<ABCEvent> toreturn = new ArrayList<>();
        toreturn.add(l);

        counter++;

        return toreturn;
    }
}

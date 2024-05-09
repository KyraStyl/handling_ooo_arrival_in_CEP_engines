package managers;

import events.ABCEvent;

import java.util.ArrayList;
import java.util.HashMap;

public class ResultManager {

    private int counter =0;
    private ArrayList<ArrayList<ABCEvent>> matches;

    private HashMap<Integer, Boolean> emitted; //integer of match, and true or false whether it is emitted to the user
    private HashMap<Integer, Boolean> updated; //integer of match, and true or false whether it is updated or not
    private HashMap<Integer, Boolean> ooo; //integer of match, and true or false whether it is ooo or not

    public ResultManager(){
        this.counter = 0;
        this.matches = new ArrayList<>();
        this.emitted = new HashMap<>();
        this.updated = new HashMap<>();
        this.ooo = new HashMap<>();
    }

    public void acceptMatch(ArrayList<ABCEvent> m, boolean oooflag){
        this.matches.add(m);
        this.emitted.put(counter,true);
        this.updated.put(counter,false);
        this.ooo.put(counter,oooflag);
        this.counter++;

        if (!oooflag)
            System.out.print("THERE IS A NEW MATCH! [");
        else
            System.out.print("OUT-OF-ORDER MATCH! [");
        m.forEach(e-> System.out.print(e.getName() + " "));
        System.out.println("]");
    }

    public void updateMatch(ArrayList<ABCEvent> m, ArrayList<ABCEvent> mnew){
        int position = this.matches.indexOf(m);
        this.matches.set(position,mnew);
        this.updated.put(position, true);
        System.out.println("last match  ===  "+ m+ " \n new match  ===  "+mnew);
        this.emitted.put(position,true);
    }

}

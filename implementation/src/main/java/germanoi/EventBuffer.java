package germanoi;

import cep.sasesystem.engine.EngineController;
import events.ABCEvent;
import events.TimestampComparator;
import utils.Configs;

import java.util.PriorityQueue;

public class EventBuffer {
    private PriorityQueue<ABCEvent> buffer;
    private int aep; // already emitted pointer
    private ABCEvent aepEvent;
    private double a; // for adaptive speculation
    private int k; //max δ - maximum latency of incoming events
    private long clk;


    public EventBuffer(Configs configs) {
        this.buffer = new PriorityQueue<>(new TimestampComparator());
        this.aep = 0;
        this.aepEvent = null;
        this.k = 0;
        this.clk = 0L;
    }

    public void addEvent(ABCEvent event) {
        this.buffer.add(event);
    }

    public ABCEvent getNextEvent() {
        if (!this.buffer.isEmpty()) {
            return this.buffer.poll();
        }
        return null;
    }

    public int getK() {
        return k;
    }

    public int getAep() {
        return aep;
    }

    public long getCLK() {
        return clk;
    }

    public boolean isEmpty() {
        return this.buffer.isEmpty();
    }

    public boolean inequality_check(ABCEvent e){
        return e.getTimestampDate().getTime() + this.a*this.k <= this.clk;
    }

    public int compareToAEP(ABCEvent e){
        if (e.getTimestampDate().getTime() < aepEvent.getTimestampDate().getTime())
            return -1;
        else if (e.getTimestampDate().getTime() == aepEvent.getTimestampDate().getTime()) {
            return 0;
        }else{
            return 1;
        }
    }

    public void updateAEP(ABCEvent e){
        this.aepEvent = e;
        this.aep = this.buffer.size()-1;
    }

    public void updateCLK(ABCEvent e){
        this.clk = e.getTimestampDate().getTime();
    }

    public void updateK(ABCEvent e){
        this.k += (int) (e.getTimestampDate().getTime() - this.clk);
    }

    public boolean purgeEvents(){
        boolean purgeOK = true;
        for(int i=0;i<k;i++) {
            ABCEvent e = buffer.poll();
            if(e != null){
                if(!inequality_check(e)){
                    buffer.add(e);
                    purgeOK = false;
                }
            }else {
                purgeOK = false;
            }
        }
        return purgeOK;
    }

    public PriorityQueue provideSnapshot(){
        return this.buffer;
    }

    public void restoreSnapshot(){}

    public int getSize() {
        return this.buffer.size();
    }
}
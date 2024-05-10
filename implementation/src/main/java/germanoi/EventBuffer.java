package germanoi;

import events.ABCEvent;
import events.TimestampComparator;

import java.util.PriorityQueue;

public class EventBuffer {
    private PriorityQueue<ABCEvent> buffer;
    private int aep; // already emitted pointer
    private ABCEvent aepEvent;
    private double a; // for adaptive speculation
    private int k; //max δ - maximum latency of incoming events
    private long clk;

    public EventBuffer() {
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

    public boolean isEmpty() {
        return this.buffer.isEmpty();
    }

    private boolean inequality_check(ABCEvent e){
        return e.getTimestampDate().getTime() + this.a*this.k <= this.clk;
    }

    public int compareToAEP(ABCEvent e){
        return e.compareTo(this.aepEvent);
    }

    public void updateCLK(ABCEvent e){
        this.clk = e.getTimestampDate().getTime();
    }

    public boolean purgeEvents(){
        boolean purgeOK = true;
        for(int i=0;i<k;i++) {
            if(buffer.poll() == null)
                purgeOK = false;
        }
        return purgeOK;
    }

    public PriorityQueue provideSnapshot(){
        return this.buffer;
    }

    public void restoreSnapshot(){}
}
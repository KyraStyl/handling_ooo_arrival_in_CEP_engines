package stats;

import cep.sasesystem.engine.ConfigFlags;

import java.util.Locale;

public class Profiling {

    private long maxLatency;
    private long minLatency;
    private long avgLatency;

    private int numOfMatches;

    private long memoryUsed;
    private long maxMemoryUsed;

    private int numberOfEvents;
    private String solution;

    public Profiling(String solution){
        maxLatency = Long.MIN_VALUE;
        minLatency = Long.MAX_VALUE;
        avgLatency = Long.MIN_VALUE;

        numOfMatches = 0;

        memoryUsed = 0;
        maxMemoryUsed = 0;

        numberOfEvents = 0;
        this.solution = solution;
    }

    private void updateMaxLatency(long latency){
        maxLatency = latency > maxLatency ? latency : maxLatency;
    }

    private void updateMinLatency(long latency){
        minLatency = latency < minLatency ? latency : minLatency;
    }

    private void updateAvgLatency(long latency){
        avgLatency = ((numOfMatches-1) * avgLatency + latency) / numOfMatches;
    }

    private void updateLatency(long l){
        updateMaxLatency(l);
        updateMinLatency(l);
        updateAvgLatency(l);
    }

    public void updateProfiling(long latency){
        numOfMatches ++;
        memoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        maxMemoryUsed = memoryUsed > maxMemoryUsed ? memoryUsed : maxMemoryUsed;
        updateLatency(latency);
    }

    public void printProfiling(){

        System.out.println();
        System.out.println("**************Profiling Numbers*****************");
//        System.out.println("Total Running Time: " + ((double)totalRunTime  / 1_000_000_000)+" seconds");
        System.out.println("Solution: "+ this.solution);
        System.out.println("Number Of Events Processed: " + numberOfEvents);
        System.out.println("Number Of Matches Found: " + numOfMatches);
        System.out.println("Used memory is bytes: " + memoryUsed);
        System.out.println("Used memory is megabytes: " + memoryUsed/(1024L*1024L));

        System.out.println("Maximum Latency in nano: " + maxLatency);
        System.out.println("Minimum Latency in nano: " + minLatency);



        if (numOfMatches > 0)
            System.out.println("Average Latency in nano: " + avgLatency);
        else
            System.out.println("No matches found!");


//        long throughput1 = 0;
//        if(totalRunTime > 0){
//            throughput1 = numberOfEvents* 1000000000/totalRunTime ;
//            System.out.println("Throughput: " + throughput1 + " events/second");
//        }
    }

    public void increaseEvents() {
        numberOfEvents++;
    }
}

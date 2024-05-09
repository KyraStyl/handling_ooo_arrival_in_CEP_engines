package stats;

import events.ABCEvent;

import java.util.ArrayList;
import java.util.HashMap;

public class StatisticManager {

    public int numberOfEventsProcessed = 0;
    public int numerOfEventsOOO = 0;
    public HashMap<String, Integer> numberOfOOOPerSource;
    public HashMap<String, Integer> numberOfEventsPerSource;

    public double avgOutOfOrderness;
    public double maxOutOfOrderness;
    public double minOutOfOrderness;

    public HashMap<String, Double> avgOutOfOrdernessPerSource;
    public HashMap<String, Double> maxOutOfOrdernessPerSource;
    public HashMap<String, Double> minOutOfOrdernessPerSource;

    public HashMap<String, Double> avgOOOScorePerSource;

    public HashMap<String, Long> actualArrivalRate;
    public HashMap<String, Long> estimatedArrivalRate;

    public double a = 0.6;
    public double b = 0.2;
    public double c = 0.2;

    public StatisticManager(){}

    public StatisticManager(double a, double b, double c){
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public void initializeManager(ArrayList<String> sources, HashMap<String, Long> estimatedArrivalRate){
        this.estimatedArrivalRate = estimatedArrivalRate;
        this.initializeManager(sources);
    }

    public void initializeManager(ArrayList<String> sources){
        avgOutOfOrderness = 0;
        maxOutOfOrderness = 0;
        minOutOfOrderness = Double.MAX_VALUE;

        numberOfEventsPerSource = new HashMap<>();
        numberOfOOOPerSource = new HashMap<>();

        avgOOOScorePerSource = new HashMap<>();

        avgOutOfOrdernessPerSource = new HashMap<>();
        maxOutOfOrdernessPerSource = new HashMap<>();
        minOutOfOrdernessPerSource = new HashMap<>();

        if (actualArrivalRate == null)
            actualArrivalRate = new HashMap<>();
        if (estimatedArrivalRate == null)
            estimatedArrivalRate = new HashMap<>();

        for(String source: sources){
            avgOutOfOrdernessPerSource.put(source, 0.0);
            maxOutOfOrdernessPerSource.put(source, 0.0);
            minOutOfOrdernessPerSource.put(source, Double.MAX_VALUE);
            avgOOOScorePerSource.put(source,0.0);
            numberOfEventsPerSource.put(source,0);
            numberOfOOOPerSource.put(source,0);
        }
        // initialize all values properly
        // estimated arrival rates will be given by the user
    }

    public void setParameters(double a1, double b2, double c3){
        a = a1;
        b = b2;
        c = c3;
    }

    //Calculates the out-of-orderness score for a specific event
    public double calculateScore(ABCEvent e, String source, ABCEvent last, Long timeWindow){

        double differenceArrivalRate = calculateDifferenceArrivalRate(source);
        double timeDifference = calculateTimeDifference(e, source, last);
        double windowPercent = actualArrivalRate.get(source) / timeWindow;

        double score = a * timeDifference + b * differenceArrivalRate + c * windowPercent;

        return score;
    }

    private double calculateDifferenceArrivalRate(String source) {
        long actualRate = actualArrivalRate.get(source);
        long estimatedRate = estimatedArrivalRate.get(source);
        return Math.abs(actualRate - estimatedRate);
    }

    private double calculateTimeDifference(ABCEvent e, String source, ABCEvent last) {
        return Math.abs(e.getTimestamp().getTime()- last.getTimestamp().getTime() - actualArrivalRate.get(source));
    }

    public double calculateThreshold(String source){
        return avgOOOScorePerSource.get(source) * 2.3;
    }

    public void setEstimated(HashMap<String, Long> estimated) {
        this.estimatedArrivalRate = estimated;
        this.actualArrivalRate = estimated;
    }

    public void processUpdateStats(ABCEvent e, double score, double timediff, String source, boolean isOOO) {
        numberOfEventsPerSource.put(source, numberOfEventsPerSource.get(source) + 1);
        numberOfEventsProcessed ++;
        if(isOOO){
            if(timediff > maxOutOfOrdernessPerSource.get(source))
                maxOutOfOrdernessPerSource.put(source,timediff);

            if(timediff < minOutOfOrdernessPerSource.get(source))
                minOutOfOrdernessPerSource.put(source,timediff);

            numerOfEventsOOO ++;
            numberOfOOOPerSource.put(source, numberOfOOOPerSource.get(source)+1);

            int num = numberOfOOOPerSource.get(source);
            double newavg = ( (num-1) * avgOOOScorePerSource.get(source) + score) / num;
            avgOOOScorePerSource.put(source,newavg);
        }

        int numT = numberOfEventsPerSource.get(source);
        double newavgT = ( (numT-1) * avgOutOfOrdernessPerSource.get(source) + timediff) / numT;
        avgOutOfOrdernessPerSource.put(source,newavgT);

    }
}

package germanoi;
import events.ABCEvent;
import utils.Configs;

import java.util.Random;

public class SpeculativeProcessor {
    private EventBuffer buffer;
    private double alpha;  // Degree of speculation, 1.0 means no speculation.
    private Random random;

    public SpeculativeProcessor(Configs configs) {
        buffer = new EventBuffer(configs);
        alpha = 1.0;  // Start with no speculation
        random = new Random();
    }

    public void processEvent(ABCEvent event) {
        buffer.addEvent(event);
        while (!buffer.isEmpty() && shouldSpeculate()) {
            speculate();
        }
    }

    private boolean shouldSpeculate() {
        // Speculation condition based on the system load or other metrics
        // For simplicity, we randomize the decision
        return random.nextDouble() < alpha; // Less alpha, more likely to speculate
    }

    private void speculate() {
        ABCEvent event = buffer.getNextEvent();
        if (event != null) {
            // Process the event speculatively
            System.out.println("Speculatively processing: " + event.getType());

            // Check if speculation was correct
            if (random.nextInt(10) < 2) { // 20% chance the speculation is wrong
                recoverFromError();
            }
        }
    }

    private void recoverFromError() {
        System.out.println("Error in speculation detected, rolling back...");
        // Implement rollback or recovery logic
        // This could mean resetting the state of the application to before the speculative event was processed
    }

    public void adaptSpeculation() {
        // Dynamically adjust the alpha value based on system load
        // For simulation, we'll just adjust alpha randomly
        alpha *= (0.5 + random.nextDouble()); // Randomly increase or decrease speculation
        System.out.println("Adjusted alpha to: " + alpha);
    }
}
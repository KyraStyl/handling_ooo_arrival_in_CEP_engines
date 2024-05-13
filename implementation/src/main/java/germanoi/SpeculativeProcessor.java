package germanoi;
import cep.sasesystem.engine.EngineController;
import events.ABCEvent;
import stats.Profiling;
import utils.Configs;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.PriorityQueue;

public class SpeculativeProcessor {
    private EventBuffer buffer;
    private double alpha;  // Degree of speculation, 1.0 means no speculation.
    private EngineController engineController;
    private Configs configs;
    private long latest_arrived;
    private Profiling profiling;

    public SpeculativeProcessor(Configs configs) {
        this.profiling = new Profiling("Germanoi");
        this.buffer = new EventBuffer(configs);
        this.alpha = 0.0;  // Start with speculation
        this.configs = configs;
        this.engineController = new EngineController();
        this.initializeEngine();
        this.latest_arrived = 0L;
    }

    public void initializeEngine(){
        this.engineController.setConfigs(this.configs);
        this.engineController.setNfa(this.configs.nfaFileLocation());
        this.engineController.setEngine();
        this.engineController.initializeEngine();
        this.configs.setStates(this.engineController.getStates());
        this.configs.setWindowLength(this.engineController.getWindow());
    }

    public void processEvent(ABCEvent event) {
        long current_ts = event.getTimestampDate().getTime();
        latest_arrived = current_ts > latest_arrived ? current_ts : latest_arrived; //keep track of the max long ts received - maybe unnecessary?

        if(terminate(event)) {
            profiling.printProfiling();
        }

        if (shouldUpdateCLK(event)) {
            this.buffer.updateCLK(event);
        }

        if (shouldBuffer(event)){
            if(buffer.getSize()+1 > buffer.getK()){
                for(int i=0;i<buffer.getSize() - buffer.getK() - 1;i++) {
                    speculate();
                }
            }
            buffer.addEvent(event);
        }


        while (!buffer.isEmpty() && !shouldBuffer(event)) {
            speculate();
        }

        if(shouldPurgeEvents(event))
            this.buffer.purgeEvents();
    }

    private boolean terminate(ABCEvent event) {
        return event.getName().equalsIgnoreCase("terminate");
    }

    private boolean shouldUpdateCLK(ABCEvent e){
        return e.getType().equalsIgnoreCase(this.configs.last_state());
    }

    private boolean shouldBuffer(ABCEvent e) {
        // ei.ts + a*K <= clk && buffer_size + 1 <= K
        return this.buffer.inequality_check(e); // Less alpha, more likely to speculate
    }

    private int shouldUpdateAlpha(){
        // Speculation condition based on the system load or other metrics
        // check cpu usage
        // if high cpu usage -> buffer -> increase alpha
        // return 1
        // if low cpu usage -> speculate -> decrease alpha
        // return -1

        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        CentralProcessor cpu = hal.getProcessor();

        double load = cpu.getSystemCpuLoad(10) * 100; // Get CPU load
        System.out.printf("CPU Usage: %.2f%%\n", load);

        if (load > 20)
            return 1;
        if (load < 10)
            return -1;
        return 0;
    }

    private boolean shouldPurgeEvents(ABCEvent e){
        return this.buffer.getSize() >= this.buffer.getK();
    }

    private void speculate() {
        ABCEvent event = buffer.getNextEvent();
        if (event != null) {
            this.buffer.updateAEP(event);
            // Process the event speculatively
            System.out.println("Speculatively processing: " + event.getType());
            engineController.runSASEonce(event, profiling);
            // Check if speculation was correct
            if(missSpeculationDetected(event)) {
                adaptSpeculation(event);
            }
        }
    }

    private boolean missSpeculationDetected(ABCEvent event) {
        return this.buffer.compareToAEP(event) < 1;
    }

    private void recoverFromError() {
        System.out.println("Error in speculation detected, rolling back...");
        // Implement rollback or recovery logic
        // This could mean resetting the state of the application to before the speculative event was processed
    }

    public void adaptSpeculation(ABCEvent e) {
        // Dynamically adjust the alpha value based on system load
        // For simulation, we'll just adjust alpha randomly
        this.buffer.updateAEP(e);
        int det = shouldUpdateAlpha(); //if shouldUpdate == 0 then same alpha
        this.alpha += det*0.5;
        this.alpha = this.alpha > 1? 1: this.alpha;
        this.alpha = this.alpha < 0? 0: this.alpha;
        this.buffer.updateK(e);
        System.out.println("Adjusted alpha to: " + alpha);
        restoreSnapshot();
    }

    private void restoreSnapshot(){
        PriorityQueue snapshot = this.buffer.provideSnapshot();
//        for(ABCEvent e: snapshot.)
    }

    public void updateProfiling(){
        this.profiling.updateProfiling(1L);
    }
}
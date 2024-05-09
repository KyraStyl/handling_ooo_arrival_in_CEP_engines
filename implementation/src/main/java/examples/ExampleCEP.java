package examples;

import events.Source;

import java.util.ArrayList;
import java.util.HashMap;

public interface ExampleCEP {

    void initializeExample();

    ArrayList<Source> getSources();
    HashMap<String,Long> getEstimated();
}

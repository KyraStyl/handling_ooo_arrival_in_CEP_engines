package events;

import java.util.Date;

public class KeyValueEvent<T> extends ABCEvent{

    private T value;

    public KeyValueEvent(String name, Date timestamp, String source, String type, String key, T value) {
        super(name, timestamp.toString(), source, type);
        this.value = value;
    }

    public KeyValueEvent(String name, String timestamp, String source, String type, String key, T value) {
        super(name, timestamp, source, type);
        this.value = value;
    }
}

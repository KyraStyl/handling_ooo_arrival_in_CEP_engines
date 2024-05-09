package events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.Objects;

import static utils.UsefulFunctions.*;

public class ABCEvent {
    private Long id;
    private String name;
    private Date timestamp;
    private static Long idcounter = 0L;
    private String source;
    private String type;

    @JsonCreator
    public ABCEvent(@JsonProperty("name") String name, @JsonProperty("timestamp") Date timestamp, @JsonProperty("source") String source, String type){
        this.type = type;
        this.id = idcounter;
        this.name = name;
        this.timestamp = timestamp;
        this.source = source;
        idcounter++;
    }

    public ABCEvent(@JsonProperty("name") String name, @JsonProperty("timestamp") String timestamp, @JsonProperty("source") String source, String type){
        this.type = type;
        this.id = idcounter;
        this.name = name;
        this.timestamp = castStrToDate(timestamp);
        this.source = source;
        idcounter++;
    }

    public ABCEvent(@JsonProperty("name") String name, @JsonProperty("timestamp") String timestamp, String type){
        this(name,castStrToDate(timestamp), "none", type);
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public Date getTimestamp(){ return timestamp; }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "ABCEvent{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ABCEvent)) return false;
        ABCEvent abcEvent = (ABCEvent) o;
        return Objects.equals(getId(), abcEvent.getId()) && Objects.equals(getName(), abcEvent.getName()) && Objects.equals(getTimestamp(), abcEvent.getTimestamp()) && Objects.equals(getSource(), abcEvent.getSource()) && Objects.equals(getType(), abcEvent.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getName(), getTimestamp(), getSource(), getType());
    }
    
    public int compareTo(ABCEvent other) {
        return this.timestamp.compareTo(other.timestamp);
    }
}
package events;

import java.util.Comparator;
import java.util.Date;

public class TimestampComparator implements Comparator<ABCEvent> {

    @Override
    public int compare(ABCEvent o1, ABCEvent o2) {
//        System.out.println("COMPARING "+o1.toString()+" WITH "+o2.toString());
        Date o1date = o1.getTimestamp();
        Date o2date = o2.getTimestamp();
        return o1date.compareTo(o2date);
    }
}
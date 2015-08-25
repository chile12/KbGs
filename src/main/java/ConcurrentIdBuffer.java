
import akka.util.ConcurrentMultiMap;
import scala.collection.Iterator;
import scala.collection.Iterable;
import scala.collection.immutable.List;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

/**
 * Created by Chile on 8/24/2015.
 */
public class ConcurrentIdBuffer {

    private Iterator<String> stremResource;
    private ConcurrentMultiMap<String, String> map;
    private int buffer;

    public ConcurrentIdBuffer(Iterator<String> stremResource, int bufferSize)
    {
        this.buffer = bufferSize;
        this.map = new ConcurrentMultiMap<String, String>(bufferSize, new ExampleComparator());
        this.stremResource = stremResource;
        for (int i =0; i < bufferSize; i++) {
            addFromResource(stremResource);
        }
    }

    private synchronized void addFromResource(Iterator<String> sr)
    {
            String inp = sr.next();
            String uri = inp.substring(0, inp.indexOf(" - "));
            String id = inp.substring(inp.indexOf(" - ") + 3);
            map.put(uri, id);
    }

    public synchronized void remove(String uri)
    {
            Iterable<String> rem = map.remove(uri).get();
            for (int i = getKeys().size(); i < this.buffer; i++) {
                if(stremResource.hasNext())
                    addFromResource(stremResource);
            }
    }

    public synchronized List<String> getValues(String key)
    {
        return map.valueIterator(key).toList();
    }

    public synchronized List<String> getKeys()
    {
        return map.keys().toList();
    }

    public synchronized boolean contains(String key)
    {
        List<String> itr = map.keys().toList();
        if(itr.contains(key))
            return true;
        else
            return false;
    }

    public synchronized int size()
    {
        return map.mapSize();
    }

    public class ExampleComparator  implements Comparator<String> {
        public int compare(String obj1, String obj2) {
            if (obj1 == obj2) {
                return 0;
            }
            if (obj1 == null) {
                return -1;
            }
            if (obj2 == null) {
                return 1;
            }
            return obj1.compareTo(obj2);
        }
    }
}

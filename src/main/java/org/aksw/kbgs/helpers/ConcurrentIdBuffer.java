package org.aksw.kbgs.helpers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multiset;
import scala.collection.Iterator;
import scala.collection.immutable.List;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

/**
 * Created by Chile on 8/24/2015.
 */
public class ConcurrentIdBuffer {

    private Iterator<String> stremResource;
    private HashMultimap<String, String> map;
    private HashMultimap<String, String> sameAsIds;
    private int buffer;

    public ConcurrentIdBuffer(Iterator<String> stremResource, int bufferSize)
    {
        this.buffer = bufferSize;
        this.map = HashMultimap.create();
        this.sameAsIds = HashMultimap.create();
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
            map.removeAll(uri);
            for (int i = getKeys().size(); i < this.buffer; i++) {
                if(stremResource.hasNext())
                    addFromResource(stremResource);
            }
    }

    public synchronized List<String> getValues(String key)
    {
        return scala.collection.JavaConverters.asScalaSetConverter(map.get(key)).asScala().toList();
    }

    public synchronized List<String> getKeys()
    {
        return scala.collection.JavaConverters.asScalaSetConverter(map.keySet()).asScala().toList();
    }

    public synchronized boolean contains(String key)
    {
        Multiset<String> itr = map.keys();
        if(itr.contains(key))
            return true;
        else
            return false;
    }

    public synchronized int size()
    {
        return map.size();
    }

    public synchronized void addSameAs(String key, String value)
    {
        this.sameAsIds.put(key, value);
    }

    public synchronized List<String> getSameAs(String key)
    {
        return scala.collection.JavaConverters.asScalaSetConverter(this.sameAsIds.get(key)).asScala().toList();
    }

    public HashMultimap<String, String> getSameAsMap()
    {
        return this.sameAsIds;
    }

    public void addSameAsBuffer(ConcurrentIdBuffer idBuffer)
    {
        this.sameAsIds.putAll(idBuffer.getSameAsMap());
    }

    /**
     * to remove occurrences of x -> y & y -> x
     */
    public void normalizeSameAsLinks()
    {
        ArrayList<Map.Entry<String, String>> removeList = new ArrayList<Map.Entry<String, String>>();
        for(Map.Entry<String, String> entry : sameAsIds.entries()){
            if(sameAsIds.keySet().contains(entry.getValue()))
                removeList.add(entry);
        }
        for(Map.Entry<String, String> entry : removeList)
            sameAsIds.remove(entry.getKey(), entry.getValue());
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

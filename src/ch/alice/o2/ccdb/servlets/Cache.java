package ch.alice.o2.ccdb.servlets;

import java.util.HashMap;
import java.util.Map;

public class Cache {
    private boolean active;

    private final Map<String, Integer> valueToID = new HashMap<>();
    private final Map<Integer, String> IDToValue = new HashMap<>();

    public Cache(boolean cacheActive) {
        this.active = cacheActive;
    }

    public Integer getIdFromCache(String value) {
        if(active) {
            return valueToID.get(value);
        }
        return null;
    }

    public String getValueFromCache(Integer id) {
        if(active) {
            return IDToValue.get(id);
        }
        return null;
    }

    public void putInCache(Integer id, String value) {
        if(active) {
            valueToID.put(value, id);
            IDToValue.put(id, value);
        }
    }

    public String removeById(Integer id) {
        final String value = IDToValue.remove(id);

        if (value != null)
            valueToID.remove(value);

        return value;
    }
}

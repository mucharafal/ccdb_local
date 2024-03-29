package ch.alice.o2.ccdb.servlets;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author rmucha
 * @since 2021-08-09
 */
public class Cache {
	private final boolean active;

	private final Map<String, Integer> valueToID = new ConcurrentHashMap<>();
	private final Map<Integer, String> IDToValue = new ConcurrentHashMap<>();

	/**
	 * @param cacheActive set to `false` on the Online instance (multi-master-aware)
	 */
	public Cache(final boolean cacheActive) {
		this.active = cacheActive;
	}

	/**
	 * @param value
	 * @return the known ID for the given value
	 */
	public Integer getIdFromCache(final String value) {
		if (active && value != null) {
			return valueToID.get(value);
		}
		return null;
	}

	/**
	 * @param id
	 * @return the value associated to the given ID, if known
	 */
	public String getValueFromCache(final Integer id) {
		if (active && id != null) {
			return IDToValue.get(id);
		}
		return null;
	}

	/**
	 * Set a (key, value) pair
	 * 
	 * @param id
	 * @param value
	 */
	public void putInCache(final Integer id, final String value) {
		if (active && id != null && value != null) {
			synchronized (this) {
				valueToID.put(value, id);
				IDToValue.put(id, value);
			}
		}
	}

	/**
	 * @param id
	 * @return the old value, if any was associated in the cache to this ID
	 */
	public String removeById(final Integer id) {
		if (id != null) {
			synchronized (this) {
				final String value = IDToValue.remove(id);

				if (value != null)
					valueToID.remove(value);

				return value;
			}
		}

		return null;
	}

	void clear() {
		synchronized (this) {
			valueToID.clear();
			IDToValue.clear();
		}
	}
}

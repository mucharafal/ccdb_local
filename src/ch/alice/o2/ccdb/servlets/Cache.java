package ch.alice.o2.ccdb.servlets;

import java.util.HashMap;
import java.util.Map;

/**
 * @author rmucha
 * @since 2021-08-09
 */
public class Cache {
	private final boolean active;

	private final Map<String, Integer> valueToID = new HashMap<>();
	private final Map<Integer, String> IDToValue = new HashMap<>();

	/**
	 * @param cacheActive set to `true` on the Online instance
	 */
	public Cache(final boolean cacheActive) {
		this.active = cacheActive;
	}

	/**
	 * @param value
	 * @return the known ID for the given value
	 */
	public Integer getIdFromCache(final String value) {
		if (active) {
			return valueToID.get(value);
		}
		return null;
	}

	/**
	 * @param id
	 * @return the value associated to the given ID, if known
	 */
	public String getValueFromCache(final Integer id) {
		if (active) {
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
		if (active) {
			valueToID.put(value, id);
			IDToValue.put(id, value);
		}
	}

	/**
	 * @param id
	 * @return the old value, if any was associated in the cache to this ID
	 */
	public String removeById(final Integer id) {
		final String value = IDToValue.remove(id);

		if (value != null)
			valueToID.remove(value);

		return value;
	}
}

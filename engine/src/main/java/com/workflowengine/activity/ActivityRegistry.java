package com.workflowengine.activity;

import com.workflowengine.api.activity.Activity;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Indexes Spring {@link Activity} beans by {@link Activity#name()}.
 *
 * <p>Built once at startup; the map is then immutable. Duplicate names fail
 * the context. Lookups are safe from executor threads. A miss is not an
 * exception; the invoker converts it to {@code UNKNOWN_ACTIVITY}.
 */
@Component
public class ActivityRegistry {

    private final Map<String, Activity> byName;

    /**
     * Indexes every activity in the context.
     *
     * @param activities Spring-discovered activities; names must be unique
     * @throws IllegalStateException if two beans share a name
     */
    public ActivityRegistry(List<Activity> activities) {
        Map<String, Activity> map = new HashMap<>();
        for (Activity activity : activities) {
            Activity previous = map.put(activity.name(), activity);
            if (previous != null) {
                throw new IllegalStateException("duplicate activity: " + activity.name());
            }
        }
        this.byName = Map.copyOf(map);
    }

    /**
     * Finds an activity by step name.
     *
     * @param name step name; may be null
     * @return empty if unknown
     */
    public Optional<Activity> findByName(String name) {
        return Optional.ofNullable(byName.get(name));
    }
}

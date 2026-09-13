package com.workflowengine.activity;

import com.workflowengine.api.activity.Activity;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class ActivityRegistry {

    private final Map<String, Activity> byName;

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

    public Optional<Activity> findByName(String name) {
        return Optional.ofNullable(byName.get(name));
    }
}

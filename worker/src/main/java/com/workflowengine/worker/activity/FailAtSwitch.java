package com.workflowengine.worker.activity;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Enables {@code failAt} only while this process has the {@code test} profile.
 *
 * <p>A non-test worker leaves the switch off, so a client cannot force a
 * stub failure. Destroy turns it off again so a later context in the same
 * JVM does not inherit the test setting. Singleton, no per-request state.
 */
@Component
class FailAtSwitch implements DisposableBean {

    /**
     * Reads the active profiles once at startup.
     *
     * @param environment Spring environment for this worker process
     */
    FailAtSwitch(Environment environment) {
        FailAt.setHonor(environment.matchesProfiles("test"));
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        FailAt.setHonor(false);
    }
}

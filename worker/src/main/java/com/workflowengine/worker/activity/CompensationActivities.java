package com.workflowengine.worker.activity;

import com.workflowengine.api.activity.Activity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Canned compensation activities for the ORDER saga.
 *
 * <p>Each bean is one activity name the engine publishes after a later forward
 * step fails. They do not talk to inventory or payments. {@code failAt} is
 * honored through {@link StubSupport} when the test profile is on. Spring
 * singleton. The listener thread calls {@link Activity#execute}.
 */
@Configuration
public class CompensationActivities {

    /**
     * @return stub for {@code COMPENSATE_CREATE_ORDER}
     */
    @Bean
    public Activity compensateCreateOrder() {
        return stub("COMPENSATE_CREATE_ORDER");
    }

    /**
     * @return stub for {@code COMPENSATE_RESERVE_INVENTORY}
     */
    @Bean
    public Activity compensateReserveInventory() {
        return stub("COMPENSATE_RESERVE_INVENTORY");
    }

    /**
     * @return stub for {@code COMPENSATE_PROCESS_PAYMENT}
     */
    @Bean
    public Activity compensateProcessPayment() {
        return stub("COMPENSATE_PROCESS_PAYMENT");
    }

    /**
     * @return stub for {@code COMPENSATE_CREATE_SHIPMENT}
     */
    @Bean
    public Activity compensateCreateShipment() {
        return stub("COMPENSATE_CREATE_SHIPMENT");
    }

    /**
     * @return stub for {@code COMPENSATE_SEND_NOTIFICATION}
     */
    @Bean
    public Activity compensateSendNotification() {
        return stub("COMPENSATE_SEND_NOTIFICATION");
    }

    private static Activity stub(String name) {
        return new Activity() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public com.workflowengine.api.activity.ActivityResult execute(
                    com.workflowengine.api.activity.ActivityContext context) {
                return StubSupport.execute(name, context);
            }
        };
    }
}

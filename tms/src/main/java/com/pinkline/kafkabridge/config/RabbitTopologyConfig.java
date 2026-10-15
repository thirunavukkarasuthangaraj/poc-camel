package com.pinkline.kafkabridge.config;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Self-healing RabbitMQ topology for the SCADA → TMS queues.
 *
 * The SCADA-side broker has no persistent storage, so a pod restart erases
 * queues and bindings and SCADA messages become unroutable with no error
 * (2026-07-07 outage). Spring's RabbitAdmin re-declares every Declarables bean
 * on each (re)connect; durable queue/binding declaration is idempotent.
 *
 * Exchanges are not declared: amq.* are broker built-ins and RabbitMQ refuses
 * a re-declare.
 */
@Configuration
public class RabbitTopologyConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitTopologyConfig.class);

    @Bean
    public Declarables bridgeRabbitTopology(BridgeConfig config) {
        List<Declarable> declarables = new ArrayList<>();
        for (BridgeConfig.ScadaSource src : config.getScadaSource()) {
            if (src.getQueue() == null || src.getQueue().isBlank()) {
                continue;
            }
            declarables.add(new Queue(src.getQueue(), true, false, false));
            declarables.add(new Binding(src.getQueue(), Binding.DestinationType.QUEUE,
                    src.getFromExchange(), src.getRoutingKey(), null));
            log.info("RabbitTopology — queue [{}] bound to [{}] with key [{}]",
                    src.getQueue(), src.getFromExchange(), src.getRoutingKey());
        }
        return new Declarables(declarables);
    }
}

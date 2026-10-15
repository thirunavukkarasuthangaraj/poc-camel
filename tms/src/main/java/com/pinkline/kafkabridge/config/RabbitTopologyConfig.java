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
 * RabbitTopologyConfig — self-healing RabbitMQ topology.
 *
 * The broker in this deployment has no persistent storage: every RabbitMQ
 * pod restart/rebuild silently erases all queues and bindings, and the
 * bridge's spring-rabbitmq consumers only declare passively — so after a
 * broker rebuild, SCADA messages were dropped as unroutable with no error
 * anywhere (see the 2026-07-07 outage: missing queue, then a dead
 * hand-created binding).
 *
 * Spring's RabbitAdmin declares every {@link Declarables} bean each time a
 * connection is (re)established — on app startup AND on reconnect after a
 * broker restart. Durable-queue/binding declaration is idempotent, so this
 * is safe when the topology already exists.
 *
 * Exchanges are NOT declared here: amq.* exchanges are broker built-ins and
 * re-declaring them is refused by RabbitMQ.
 */
@Configuration
public class RabbitTopologyConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitTopologyConfig.class);

    @Bean
    public Declarables bridgeRabbitTopology(BridgeConfig config) {
        List<Declarable> declarables = new ArrayList<>();

        // SCADA → TMS entry queues (Stage B sources)
        for (BridgeConfig.ScadaSource src : config.getScadaSource()) {
            if (src.getQueue() == null || src.getQueue().isBlank()) {
                continue;
            }
            declarables.add(new Queue(src.getQueue(), true, false, false));
            declarables.add(new Binding(src.getQueue(), Binding.DestinationType.QUEUE,
                    src.getFromExchange(), src.getRoutingKey(), null));
            log.info("RabbitTopology — will declare queue [{}] bound to [{}] with key [{}]",
                    src.getQueue(), src.getFromExchange(), src.getRoutingKey());
        }

        // SCADA -> TMS inbound queues (bridge.inbound[n]) - previously declared by the
        // now-removed RabbitmqTopologyConfig, which also (wrongly) re-declared the
        // built-in amq.topic exchange. Declared here so they survive a broker rebuild.
        for (BridgeConfig.InboundRoute inb : config.getInbound()) {
            if (inb.getQueue() == null || inb.getQueue().isBlank()) {
                continue;
            }
            String exchange = (inb.getFromExchange() == null || inb.getFromExchange().isBlank())
                    ? "amq.topic" : inb.getFromExchange();
            declarables.add(new Queue(inb.getQueue(), true, false, false));
            declarables.add(new Binding(inb.getQueue(), Binding.DestinationType.QUEUE,
                    exchange, inb.getRoutingKey(), null));
            log.info("RabbitTopology — will declare inbound queue [{}] bound to [{}] with key [{}]",
                    inb.getQueue(), exchange, inb.getRoutingKey());
        }

        // Monitor queue (only when the monitor route is enabled)
        BridgeConfig.Monitor mon = config.getMonitor();
        if (mon.isEnabled() && mon.getQueue() != null && !mon.getQueue().isBlank()) {
            declarables.add(new Queue(mon.getQueue(), true, false, false));
            declarables.add(new Binding(mon.getQueue(), Binding.DestinationType.QUEUE,
                    mon.getFromExchange(), mon.getRoutingKey(), null));
            log.info("RabbitTopology — will declare monitor queue [{}] bound to [{}] with key [{}]",
                    mon.getQueue(), mon.getFromExchange(), mon.getRoutingKey());
        }

        return new Declarables(declarables);
    }
}

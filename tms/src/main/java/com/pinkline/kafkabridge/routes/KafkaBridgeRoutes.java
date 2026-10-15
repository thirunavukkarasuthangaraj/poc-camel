package com.pinkline.kafkabridge.routes;

import com.pinkline.kafkabridge.config.BridgeConfig;
import com.pinkline.kafkabridge.processor.AesGcmDecryptor;
import com.pinkline.kafkabridge.processor.EncryptProcessor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.springframework.stereotype.Component;

/**
 * KafkaBridgeRoutes — four config-driven route groups, nothing else.
 *
 *  TMS → SCADA
 *   1. outbound      Artemis topic/queue → [AES-256-GCM encrypt] → Kafka
 *                    bridge.artemis-topics / bridge.artemis-queues → bridge.kafka.topic
 *   2. relay         Kafka → RabbitMQ exchange + routing key          bridge.forward[n]
 *                    (RabbitMQ's MQTT plugin hands it to SCADA; no code here)
 *  SCADA → TMS
 *   3. scada-source  RabbitMQ queue → [AES-256-GCM decrypt] → Kafka   bridge.scada-source[n]
 *   4. artemis-sink  Kafka → Artemis topic                            bridge.artemis-sink[n]
 *
 * Bodies are forwarded unchanged. The Traffic Scheduler already publishes the
 * final APIS JSON (ICD_PASIF_V3) and SCADA publishes plain RSAE JSON, so the
 * bridge never parses or converts a payload.
 */
@Component
public class KafkaBridgeRoutes extends RouteBuilder {

    private static final String BYTE_SER   = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private static final String BYTE_DESER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    private final BridgeConfig config;

    public KafkaBridgeRoutes(BridgeConfig config) {
        this.config = config;
    }

    @Override
    public void configure() {

        // Anything that fails 3 retries lands in the Artemis DLQ with the
        // inbound JMS headers stripped (a stale JMSDeliveryMode byte[] header
        // from a Kafka/RabbitMQ round-trip breaks the JMS send otherwise).
        onException(Exception.class)
            .maximumRedeliveries(3)
            .redeliveryDelay(2000)
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .useOriginalMessage()
            .removeHeaders("JMS*")
            .to("activemq:queue:DLQ.kafka-bridge")
            .log("Dead-letter queue — ${exception.message}")
            .handled(true);

        // 1. outbound: Artemis → [encrypt] → Kafka
        final boolean encrypt = config.getEncrypt().isEnabled();
        final String kafkaTopic = config.getKafka().getTopic();
        for (ArtemisSource source : artemisSources(config)) {
            RouteDefinition route = from(source.endpointUri())
                .routeId(source.routeId())
                .log("← Artemis " + source.logLabel() + " [" + source.destination()
                        + "] JMSMessageID=${header.JMSMessageID}")
                .removeHeaders("JMS*");
            if (encrypt) {
                route.process(new EncryptProcessor());
            } else {
                route.process(e -> e.getIn().setBody(
                        e.getIn().getBody(String.class).getBytes(StandardCharsets.UTF_8)));
            }
            route.to(kafkaProducer(kafkaTopic))
                 .log("→ Kafka [" + kafkaTopic + "]" + (encrypt ? " (encrypted)" : " (plain)"));
        }

        // 2. relay: Kafka → RabbitMQ
        for (BridgeConfig.ForwardRoute fwd : config.getForward()) {
            from(kafkaConsumer(fwd.getFromKafka(), fwd.getConsumerGroup(), "latest"))
                .routeId("relay-" + routeIdPart(fwd.getFromKafka()) + "-to-" + routeIdPart(fwd.getRoutingKey()))
                .log("← Kafka relay [" + fwd.getFromKafka() + "]")
                .to("spring-rabbitmq:" + fwd.getToExchange() + "?routingKey=" + fwd.getRoutingKey())
                .log("→ RabbitMQ [" + fwd.getToExchange() + " / " + fwd.getRoutingKey() + "]");
        }

        // 3. scada-source: RabbitMQ → [decrypt] → Kafka
        final boolean decrypt = config.getReverse().isDecryptEnabled();
        for (BridgeConfig.ScadaSource src : config.getScadaSource()) {
            String fromUri = "spring-rabbitmq:" + src.getFromExchange()
                    + "?routingKey=" + src.getRoutingKey() + "&autoStartup=true";
            if (src.getQueue() != null && !src.getQueue().isBlank()) {
                fromUri += "&queues=" + src.getQueue();
            }
            RouteDefinition route = from(fromUri)
                .routeId("scada-source-" + routeIdPart(src.getRoutingKey()) + "-to-kafka-" + routeIdPart(src.getToKafkaTopic()))
                .log("← SCADA [" + src.getRoutingKey() + "]");
            if (decrypt) {
                route.process(e -> e.getIn().setBody(
                        AesGcmDecryptor.decrypt(e.getIn().getBody(byte[].class))
                                .getBytes(StandardCharsets.UTF_8)));
            } else {
                route.process(e -> e.getIn().setBody(e.getIn().getBody(byte[].class)));
            }
            route.to(kafkaProducer(src.getToKafkaTopic()))
                 .log("→ Kafka [" + src.getToKafkaTopic() + "]");
        }

        // 4. artemis-sink: Kafka → Artemis
        for (BridgeConfig.ArtemisSink sink : config.getArtemisSink()) {
            from(kafkaConsumer(sink.getFromKafka(), sink.getConsumerGroup(), "earliest"))
                .routeId("artemis-sink-" + routeIdPart(sink.getFromKafka()) + "-to-" + routeIdPart(sink.getToTopic()))
                .log("← Kafka [" + sink.getFromKafka() + "]")
                .process(e -> e.getIn().setBody(
                        new String(e.getIn().getBody(byte[].class), StandardCharsets.UTF_8)))
                // Let Artemis set fresh JMS headers instead of trusting transported ones.
                .removeHeaders("JMS*")
                .to("activemq:topic:" + sink.getToTopic())
                .log("→ Artemis [" + sink.getToTopic() + "]");
        }
    }

    private static String kafkaProducer(String topic) {
        return "kafka:" + topic + "?brokers={{kafka.brokers}}"
                + "&valueSerializer=" + BYTE_SER + "&keySerializer=" + BYTE_SER;
    }

    private static String kafkaConsumer(String topic, String group, String offsetReset) {
        return "kafka:" + topic + "?brokers={{kafka.brokers}}"
                + "&groupId=" + group + "&autoOffsetReset=" + offsetReset
                + "&valueDeserializer=" + BYTE_DESER + "&keyDeserializer=" + BYTE_DESER;
    }

    static Set<String> configuredValues(String value) {
        Set<String> result = new LinkedHashSet<>();
        if (value == null || value.isBlank()) {
            return result;
        }
        for (String entry : value.split(",", -1)) {
            String trimmed = entry.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    static String routeIdPart(String destination) {
        String result = destination.toLowerCase()
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("^-+|-+$", "");
        return result.isEmpty() ? "destination" : result;
    }

    static List<ArtemisSource> artemisSources(BridgeConfig config) {
        List<ArtemisSource> sources = new ArrayList<>();
        Set<String> routeIds = new LinkedHashSet<>();
        addSourceSpecs(sources, routeIds, "topic", "TOPIC", config.getArtemisTopics());
        addSourceSpecs(sources, routeIds, "queue", "QUEUE", config.getArtemisQueues());
        return sources;
    }

    private static void addSourceSpecs(List<ArtemisSource> sources, Set<String> routeIds,
                                       String endpointType, String logLabel, String configured) {
        for (String destination : configuredValues(configured)) {
            String base = "outbound-" + endpointType + "-" + routeIdPart(destination);
            String routeId = base;
            int suffix = 2;
            while (!routeIds.add(routeId)) {
                routeId = base + "-" + suffix++;
            }
            sources.add(new ArtemisSource(endpointType, logLabel, destination,
                    "activemq:" + endpointType + ":" + destination, routeId));
        }
    }

    record ArtemisSource(String endpointType, String logLabel, String destination,
                         String endpointUri, String routeId) { }
}

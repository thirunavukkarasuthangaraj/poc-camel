package com.pinkline.kafkabridge.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * bridge.* properties. Every route is declared here; the code only reads this.
 *
 *  TMS → SCADA
 *   bridge.artemis-topics            comma-separated Artemis topics to consume
 *   bridge.artemis-queues            comma-separated named ANYCAST queues to consume (optional)
 *   bridge.encrypt.enabled           AES-256-GCM encrypt before Kafka (key: SCADA_AES_KEY)
 *   bridge.kafka.topic               Kafka topic the outbound routes write to
 *   bridge.forward[n].from-kafka / to-exchange / routing-key / consumer-group
 *
 *  SCADA → TMS
 *   bridge.scada-source[n].from-exchange / routing-key / queue / to-kafka-topic
 *   bridge.reverse.decrypt-enabled   decrypt SCADA payloads before Kafka (false = plain JSON)
 *   bridge.artemis-sink[n].from-kafka / consumer-group / to-topic
 */
@Component
@ConfigurationProperties(prefix = "bridge")
public class BridgeConfig {

    // "" (not null) so split(",") is always safe
    private String artemisTopics = "";
    private String artemisQueues = "";
    private Encrypt encrypt = new Encrypt();
    private Reverse reverse = new Reverse();
    private Kafka kafka = new Kafka();
    private List<ForwardRoute> forward = new ArrayList<>();
    private List<ScadaSource> scadaSource = new ArrayList<>();
    private List<ArtemisSink> artemisSink = new ArrayList<>();

    public String getArtemisTopics() { return artemisTopics; }
    public void setArtemisTopics(String artemisTopics) { this.artemisTopics = artemisTopics; }

    public String getArtemisQueues() { return artemisQueues; }
    public void setArtemisQueues(String artemisQueues) { this.artemisQueues = artemisQueues; }

    public Encrypt getEncrypt() { return encrypt; }
    public void setEncrypt(Encrypt encrypt) { this.encrypt = encrypt; }

    public Reverse getReverse() { return reverse; }
    public void setReverse(Reverse reverse) { this.reverse = reverse; }

    public Kafka getKafka() { return kafka; }
    public void setKafka(Kafka kafka) { this.kafka = kafka; }

    public List<ForwardRoute> getForward() { return forward; }
    public void setForward(List<ForwardRoute> forward) { this.forward = forward; }

    public List<ScadaSource> getScadaSource() { return scadaSource; }
    public void setScadaSource(List<ScadaSource> scadaSource) { this.scadaSource = scadaSource; }

    public List<ArtemisSink> getArtemisSink() { return artemisSink; }
    public void setArtemisSink(List<ArtemisSink> artemisSink) { this.artemisSink = artemisSink; }

    public static class Encrypt {
        private boolean enabled = true;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    public static class Reverse {
        private boolean decryptEnabled = false;
        public boolean isDecryptEnabled() { return decryptEnabled; }
        public void setDecryptEnabled(boolean decryptEnabled) { this.decryptEnabled = decryptEnabled; }
    }

    public static class Kafka {
        private String topic;
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

    /** Kafka → RabbitMQ */
    public static class ForwardRoute {
        private String fromKafka;
        private String toExchange;
        private String routingKey;
        private String consumerGroup;

        public String getFromKafka() { return fromKafka; }
        public void setFromKafka(String fromKafka) { this.fromKafka = fromKafka; }
        public String getToExchange() { return toExchange; }
        public void setToExchange(String toExchange) { this.toExchange = toExchange; }
        public String getRoutingKey() { return routingKey; }
        public void setRoutingKey(String routingKey) { this.routingKey = routingKey; }
        public String getConsumerGroup() { return consumerGroup; }
        public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    }

    /** RabbitMQ → Kafka */
    public static class ScadaSource {
        private String fromExchange;
        private String routingKey;
        private String queue;
        private String toKafkaTopic;

        public String getFromExchange() { return fromExchange; }
        public void setFromExchange(String fromExchange) { this.fromExchange = fromExchange; }
        public String getRoutingKey() { return routingKey; }
        public void setRoutingKey(String routingKey) { this.routingKey = routingKey; }
        public String getQueue() { return queue; }
        public void setQueue(String queue) { this.queue = queue; }
        public String getToKafkaTopic() { return toKafkaTopic; }
        public void setToKafkaTopic(String toKafkaTopic) { this.toKafkaTopic = toKafkaTopic; }
    }

    /** Kafka → Artemis */
    public static class ArtemisSink {
        private String fromKafka;
        private String consumerGroup;
        private String toTopic;

        public String getFromKafka() { return fromKafka; }
        public void setFromKafka(String fromKafka) { this.fromKafka = fromKafka; }
        public String getConsumerGroup() { return consumerGroup; }
        public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
        public String getToTopic() { return toTopic; }
        public void setToTopic(String toTopic) { this.toTopic = toTopic; }
    }
}

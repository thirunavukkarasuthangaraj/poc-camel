package com.pinkline.kafkabridge.routes;

import com.pinkline.kafkabridge.config.BridgeConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaBridgeRoutesTest {

    @Test
    void topicOnlyConfiguration() {
        BridgeConfig config = config("jms.topic.rcs.e2k.ctc.train.pas.new", "");
        List<KafkaBridgeRoutes.ArtemisSource> sources = KafkaBridgeRoutes.artemisSources(config);
        assertEquals(1, sources.size());
        assertEquals("activemq:topic:jms.topic.rcs.e2k.ctc.train.pas.new", sources.get(0).endpointUri());
        assertEquals("outbound-topic-jms-topic-rcs-e2k-ctc-train-pas-new", sources.get(0).routeId());
    }

    @Test
    void queueOnlyConfigurationUsesExistingNamedQueueUri() {
        BridgeConfig config = config("", "jms.topic.rcs.e2k.ctc.train.pas");
        List<KafkaBridgeRoutes.ArtemisSource> sources = KafkaBridgeRoutes.artemisSources(config);
        assertEquals(1, sources.size());
        assertEquals("activemq:queue:jms.topic.rcs.e2k.ctc.train.pas", sources.get(0).endpointUri());
        assertEquals("outbound-queue-jms-topic-rcs-e2k-ctc-train-pas", sources.get(0).routeId());
    }

    @Test
    void topicAndQueueRunTogetherWithUniqueIds() {
        BridgeConfig config = config("same.destination", "same.destination");
        List<KafkaBridgeRoutes.ArtemisSource> sources = KafkaBridgeRoutes.artemisSources(config);
        assertEquals(2, sources.size());
        assertNotEquals(sources.get(0).routeId(), sources.get(1).routeId());
        assertTrue(sources.get(0).routeId().startsWith("outbound-topic-"));
        assertTrue(sources.get(1).routeId().startsWith("outbound-queue-"));
    }

    @Test
    void emptyQueueConfigurationAddsNoQueueRoute() {
        assertTrue(KafkaBridgeRoutes.artemisSources(config("topic.one", "")).stream()
                .noneMatch(s -> s.endpointType().equals("queue")));
    }

    @Test
    void multipleQueuesAreSupported() {
        List<KafkaBridgeRoutes.ArtemisSource> sources =
                KafkaBridgeRoutes.artemisSources(config("", "queue.one,queue.two"));
        assertEquals(List.of("queue.one", "queue.two"),
                sources.stream().map(KafkaBridgeRoutes.ArtemisSource::destination).toList());
    }

    @Test
    void blankEntriesTrailingCommasAndDuplicatesAreIgnored() {
        List<KafkaBridgeRoutes.ArtemisSource> sources =
                KafkaBridgeRoutes.artemisSources(config(" topic.one, ,topic.one,", " , queue.one, ,"));
        assertEquals(List.of("topic.one", "queue.one"),
                sources.stream().map(KafkaBridgeRoutes.ArtemisSource::destination).toList());
    }

    @Test
    void specialCharactersProduceSafeUniqueRouteIds() {
        List<KafkaBridgeRoutes.ArtemisSource> sources =
                KafkaBridgeRoutes.artemisSources(config("", "a::b,a/b,a:b"));
        assertEquals(List.of("outbound-queue-a-b", "outbound-queue-a-b-2", "outbound-queue-a-b-3"),
                sources.stream().map(KafkaBridgeRoutes.ArtemisSource::routeId).toList());
        assertTrue(sources.stream().allMatch(s -> s.routeId().matches("[a-z0-9-]+")));
    }

    private static BridgeConfig config(String topics, String queues) {
        BridgeConfig config = new BridgeConfig();
        config.setArtemisTopics(topics);
        config.setArtemisQueues(queues);
        return config;
    }
}

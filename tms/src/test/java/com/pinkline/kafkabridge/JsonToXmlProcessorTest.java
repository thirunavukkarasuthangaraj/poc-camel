package com.pinkline.kafkabridge;

import com.pinkline.kafkabridge.processor.JsonToXmlProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the generic, schema-agnostic JSON -> XML converter used on the
 * SCADA -> TMS reverse path. Run: mvn test (no broker needed).
 */
class JsonToXmlProcessorTest {

    private JsonToXmlProcessor jsonToXml;
    private DefaultCamelContext ctx;

    @BeforeEach
    void setup() {
        jsonToXml = new JsonToXmlProcessor();
        ctx = new DefaultCamelContext();
    }

    @Test
    void jsonToXml_usesRootNameFromHeader() throws Exception {
        Exchange ex = new DefaultExchange(ctx);
        ex.getIn().setBody("{\"type\":\"UpdateAlarm\",\"creatorId\":\"ScateX\"}");
        ex.getIn().setHeader("xmlRootName", "RSAEMessage");
        jsonToXml.process(ex);

        String xml = ex.getIn().getBody(String.class);
        assertTrue(xml.startsWith("<RSAEMessage"), xml);
        assertTrue(xml.contains("<type>UpdateAlarm</type>"), xml);
        assertEquals("application/xml", ex.getIn().getHeader("Content-Type"));
    }

    @Test
    void jsonToXml_unwrapsSingleRootWhenNoHeader() throws Exception {
        Exchange ex = new DefaultExchange(ctx);
        ex.getIn().setBody("{\"RSAEMessage\":{\"type\":\"UpdateAlarm\"}}");
        jsonToXml.process(ex);

        String xml = ex.getIn().getBody(String.class);
        assertTrue(xml.startsWith("<RSAEMessage"), xml);
        assertTrue(xml.contains("<type>UpdateAlarm</type>"), xml);
    }
}

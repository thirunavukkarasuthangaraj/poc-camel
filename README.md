# PAS-SCADA Kafka Bridge

Moves messages between the TMS broker (ActiveMQ Artemis) and the SCADA broker
(RabbitMQ) through Kafka. It never parses or converts a payload: the Traffic
Scheduler already publishes the final APIS JSON, and SCADA publishes plain RSAE JSON.

```
TMS → SCADA   Artemis jms.topic.rcs.e2k.ctc.train.pas.new
                 └─ bridge: [AES-256-GCM] → Kafka tms.scada.encrypted
                      └─ bridge: → RabbitMQ amq.topic / tms.scada.pas
                           └─ external systems consume over AMQP, or MQTT tms/scada/pas

SCADA → TMS   MQTT scada/tms/alarms  (= RabbitMQ amq.topic / scada.tms.alarms)
                 └─ bridge: queue scada.tms.alarms.queue → [decrypt] → Kafka scada.tms.processed
                      └─ bridge: → Artemis SCADA.TMS.Alarms
```

Names: [TOPICS-AND-QUEUES.md](TOPICS-AND-QUEUES.md).

## Repository

| Path | What |
|---|---|
| `tms/` | the bridge — Spring Boot 3.2 + Camel 4.4, Java 17 |
| `tms/src/main/java/.../routes/KafkaBridgeRoutes.java` | the four route groups (outbound, relay, scada-source, artemis-sink) |
| `tms/src/main/java/.../config/` | `BridgeConfig` (bridge.* properties), `SecurityConfig` (Artemis TLS), `RabbitTopologyConfig` (re-declares the SCADA queue on reconnect) |
| `tms/src/main/java/.../processor/` | `EncryptProcessor`, `AesGcmDecryptor` |
| `tms/k8s/` | `00-namespace.yaml`, `deployment.yaml` (JRE image + jar from `/opt/tms-jars/bridge`), `configmap.yaml` (broker addresses), `configmap-appprops.yaml` (topics), `secret.yaml` (credentials, AES key) |
| `tms/build-jar.cmd` | builds `dist/pas-scada-bridge.jar` with Maven in Docker |
| `external-scada/` | SCADA-side RabbitMQ manifests and the `scada-api` simulator (Flask dashboard that plays the external SCADA) |

## Configuration

* **Broker addresses** — `tms/k8s/configmap.yaml` (`ARTEMIS_HOST`, `KAFKA_HOST`, `RABBITMQ_HOST`, …)
* **Credentials / AES key** — `tms/k8s/secret.yaml`
* **Topics, routing keys, encrypt on/off** — `tms/k8s/configmap-appprops.yaml`, mounted as
  `application.properties`; edit → `kubectl apply` → `rollout restart`, no image rebuild.

`bridge.encrypt.enabled=true` turns on AES-256-GCM (key `SCADA_AES_KEY`, shared with the
consumer). `bridge.reverse.decrypt-enabled=true` decrypts SCADA's messages with the same key.

## Release (jar on the nodes + plain Java 17 image)

The pod runs `10.4.1.28:4001/tms/temurin:17-jre-alpine` (imported once per node) and
starts the jar mounted from `/opt/tms-jars/bridge/pas-scada-bridge.jar` on the node.
A release is: build the jar → copy it to the three nodes → restart the pod.

**1. Build on your PC** (Maven runs in Docker; no local JDK needed)

```bat
tmsuild-jar.cmd            → dist\pas-scada-bridge.jar
```
Upload `dist\pas-scada-bridge.jar` to `/root/` on tmsserver1.

**2. On tmsserver1 — copy the jar to every node, restart**

```bash
for n in 10.4.1.23 10.4.1.24 10.4.1.25; do
  ssh $n "mkdir -p /opt/tms-jars/bridge && cp /opt/tms-jars/bridge/pas-scada-bridge.jar /opt/tms-jars/bridge/pas-scada-bridge.jar.prev 2>/dev/null; true"
  scp -q /root/pas-scada-bridge.jar $n:/opt/tms-jars/bridge/pas-scada-bridge.jar
done
kubectl -n poc-camel rollout restart deploy/pas-scada-bridge
kubectl -n poc-camel rollout status  deploy/pas-scada-bridge --timeout=300s
```

**First time only** — the Java image and the manifests:

```bash
CTR=/var/lib/rancher/rke2/bin/ctr                        # temurin-17-jre-alpine.tar.gz uploaded to /root
for n in 10.4.1.23 10.4.1.24 10.4.1.25; do
  scp -q /root/temurin-17-jre-alpine.tar.gz $n:/root/ && ssh $n "gunzip -c /root/temurin-17-jre-alpine.tar.gz | $CTR -n k8s.io images import -"
done
cd /opt/POC-Camel-Routes && git pull
mkdir -p ~/TMS-yaml/poc-camel && cp tms/k8s/*.yaml ~/TMS-yaml/poc-camel/
vi ~/TMS-yaml/poc-camel/secret.yaml                      # real credentials + SCADA_AES_KEY
kubectl apply -f ~/TMS-yaml/poc-camel/
```

Config-only change (topics, encrypt flag): edit `configmap-appprops.yaml`, `kubectl apply -f` it, `rollout restart`.

Verify: `kubectl -n poc-camel logs deploy/pas-scada-bridge --tail=200 | grep -E "Started|outbound-|relay-|scada-source-|artemis-sink-|ERROR"`

Rollback: put `pas-scada-bridge.jar.prev` back on the three nodes and `rollout restart`.

Unit tests: `docker run --rm -v "$PWD/tms:/w" -w /w maven:3.9-eclipse-temurin-17 mvn -B clean test`

## Failure handling

* 3 retries (2 s apart), then the message goes to Artemis `DLQ.kafka-bridge` and the error is logged.
* Broker connections are supervised by Camel with exponential backoff (5 s → 60 s); the pod
  stays up while a broker is down and the readiness probe (`/actuator/health/readiness`) reports it.
* The RabbitMQ queue + binding are re-declared on every reconnect, so a broker rebuild
  does not silently drop SCADA messages.

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
| `tms/k8s/` | `deployment.yaml`, `configmap.yaml` (broker addresses), `configmap-appprops.yaml` (topics), `secret.yaml` (credentials, AES key) |
| `external-scada/` | SCADA-side RabbitMQ manifests and the `scada-api` simulator (Flask dashboard that plays the external SCADA) |

## Configuration

* **Broker addresses** — `tms/k8s/configmap.yaml` (`ARTEMIS_HOST`, `KAFKA_HOST`, `RABBITMQ_HOST`, …)
* **Credentials / AES key** — `tms/k8s/secret.yaml`
* **Topics, routing keys, encrypt on/off** — `tms/k8s/configmap-appprops.yaml`, mounted as
  `application.properties`; edit → `kubectl apply` → `rollout restart`, no image rebuild.

`bridge.encrypt.enabled=true` turns on AES-256-GCM (key `SCADA_AES_KEY`, shared with the
consumer). `bridge.reverse.decrypt-enabled=true` decrypts SCADA's messages with the same key.

## Build (any machine with Docker; the production nodes have none)

```bash
docker build -t pinkline/pas-scada-bridge:latest tms/
docker build -t ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest external-scada/scada-api/
docker save pinkline/pas-scada-bridge:latest | gzip > bridge.tar.gz
```

Unit tests: `docker run --rm -v "$PWD/tms:/w" -w /w maven:3.9-eclipse-temurin-17 mvn -B clean test`

## Deploy (RKE2, images imported on every node)

```bash
CTR=/var/lib/rancher/rke2/bin/ctr
for n in 10.4.1.23 10.4.1.24 10.4.1.25; do
  scp -q bridge.tar.gz $n:/root/ && ssh $n "gunzip -c /root/bridge.tar.gz | $CTR -n k8s.io images import -"
done
kubectl -n pinkline apply -f tms/k8s/configmap.yaml -f tms/k8s/configmap-appprops.yaml -f tms/k8s/secret.yaml -f tms/k8s/deployment.yaml
kubectl -n pinkline rollout restart deploy/pas-scada-bridge
kubectl -n pinkline rollout status  deploy/pas-scada-bridge --timeout=300s
```

Verify: `kubectl -n pinkline logs deploy/pas-scada-bridge --tail=200 | grep -E "Started|outbound-|relay-|scada-source-|artemis-sink-|ERROR"`

Rollback: `kubectl -n pinkline rollout undo deploy/pas-scada-bridge`

## Failure handling

* 3 retries (2 s apart), then the message goes to Artemis `DLQ.kafka-bridge` and the error is logged.
* Broker connections are supervised by Camel with exponential backoff (5 s → 60 s); the pod
  stays up while a broker is down and the readiness probe (`/actuator/health/readiness`) reports it.
* The RabbitMQ queue + binding are re-declared on every reconnect, so a broker rebuild
  does not silently drop SCADA messages.

# Topics and queues

Every name the bridge touches. Change them in `tms/k8s/configmap-appprops.yaml`
(apply + `rollout restart`), never in code.

## TMS → SCADA

| Hop | System | Name | Notes |
|---|---|---|---|
| 1 | Artemis (TMS) | `jms.topic.rcs.e2k.ctc.train.pas.new` | Traffic Scheduler publishes the final APIS JSON (ICD_PASIF_V3) |
| 2 | Kafka | `tms.scada.encrypted` | body as-is, AES-256-GCM when `bridge.encrypt.enabled=true` |
| 3 | RabbitMQ (SCADA) | exchange `amq.topic`, routing key `tms.scada.pas` | AMQP consumers bind a queue to this key |
| 3' | MQTT (RabbitMQ plugin) | `tms/scada/pas` | same message for MQTT consumers (`.` ↔ `/`) |

## SCADA → TMS

| Hop | System | Name | Notes |
|---|---|---|---|
| 1 | MQTT / AMQP | `scada/tms/alarms` → `amq.topic` / `scada.tms.alarms` | SCADA publishes RSAE JSON |
| 2 | RabbitMQ queue | `scada.tms.alarms.queue` | declared by the bridge on every connect (durable) |
| 3 | Kafka | `scada.tms.processed` | decrypted when `bridge.reverse.decrypt-enabled=true` |
| 4 | Artemis (TMS) | `SCADA.TMS.Alarms` | TMS consumes |

## Errors

| System | Name |
|---|---|
| Artemis | `DLQ.kafka-bridge` — any message that failed 3 retries, with the Camel exception in the log |

## Kafka consumer groups

`pas-bridge-consumer` (relay), `pas-bridge-artemis-sink` (sink). Keep them stable so
offsets survive a restart.

## Quick checks

```bash
# bridge routes up
kubectl -n pinkline logs deploy/pas-scada-bridge --tail=200 | grep -E "Started|outbound-|relay-|scada-source-|artemis-sink-|ERROR"

# Artemis side
kubectl -n artemis exec artemis-0 -- /var/lib/artemis-instance/bin/artemis queue stat | grep -E "rcs.e2k.ctc.train.pas.new|SCADA.TMS.Alarms|DLQ.kafka-bridge"

# RabbitMQ side (SCADA VM mgmt UI, port 31567): queue scada.tms.alarms.queue bound to amq.topic / scada.tms.alarms
```

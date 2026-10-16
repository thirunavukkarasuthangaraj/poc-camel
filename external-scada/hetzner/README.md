# SCADA side on a standalone VM

Runs the SCADA half of the PAS pipeline on its own machine, so the two
networks are genuinely separate instead of sharing one cluster.

```
TMS network                          |  this VM
Scheduler → Artemis → Bridge → Kafka ─┼─→ RabbitMQ → SCADA API
                                      |
                        AMQP 5672 — the only crossing
```

The bridge dials **out** from TMS to this VM. Nothing here ever opens a
connection into TMS. Both directions ride that one AMQP connection, so
the firewall needs **one rule in one direction**.

---

## Before you start

Check the cluster can reach the internet at all. If this fails, stop here
and talk to whoever owns the corporate firewall:

```bash
kubectl -n pinkline run egress --rm -i --restart=Never --image=busybox -- \
  sh -c "wget -qO- -T5 http://ifconfig.me || echo NO-EGRESS"
```

It prints the public IP your cluster appears as. Note it down.

**Do not run live scheduler traffic over this.** Use the injector in step 6
to send synthetic messages. Real platform predictions leaving the Alstom
network is a data-governance question, not a technical one.

---

## 1. Copy the files

Copy the whole `external-scada` directory, because the compose file builds
the SCADA API from `../scada-api`:

```bash
scp -r external-scada root@<vm-ip>:/opt/
```

---

## 2. Set up WireGuard

This keeps the broker off the public internet entirely. Without it you are
exposing AMQP with no TLS, which will be found by scanners within hours.

**On the VM:**

```bash
apt update && apt install -y wireguard
wg genkey | tee /etc/wireguard/private.key | wg pubkey > /etc/wireguard/public.key
chmod 600 /etc/wireguard/private.key

cat > /etc/wireguard/wg0.conf <<EOF
[Interface]
Address    = 10.9.0.1/24
ListenPort = 51820
PrivateKey = $(cat /etc/wireguard/private.key)

[Peer]
# the TMS-side node
PublicKey  = <TMS_NODE_PUBLIC_KEY>
AllowedIPs = 10.9.0.2/32
EOF

systemctl enable --now wg-quick@wg0
```

**On the TMS-side node** (one of `10.4.1.23/24/25`), the mirror image,
with `Endpoint` pointing at the VM and a keepalive so the tunnel stays up
through NAT:

```bash
[Interface]
Address    = 10.9.0.2/24
PrivateKey = <TMS_NODE_PRIVATE_KEY>

[Peer]
PublicKey           = <VM_PUBLIC_KEY>
Endpoint            = <vm-public-ip>:51820
AllowedIPs          = 10.9.0.1/32
PersistentKeepalive = 25
```

On the Hetzner firewall, allow **UDP 51820** from your cluster's egress IP
only. Nothing else needs to be open.

Confirm the tunnel before going further:

```bash
wg show            # on both ends — look for a recent handshake
ping -c3 10.9.0.1  # from the TMS node
```

---

## 3. Configure

```bash
cd /opt/external-scada/hetzner
cp .env.example .env
vi .env
```

Generate real values. Do not reuse the lab credentials:

```bash
openssl rand -base64 24   # RABBITMQ_PASS
openssl rand -base64 32   # SCADA_AES_KEY
```

Set `BIND_ADDR=10.9.0.1` so the broker listens on the tunnel.

---

## 4. Start it

```bash
docker compose up -d
docker compose ps
docker compose logs -f rabbitmq
```

Wait for the health check to go green. Then confirm the listeners are on
the tunnel address and **not** on a public interface:

```bash
ss -tlnp | grep -E '5672|1883'
```

Every line should show `10.9.0.1:` or `127.0.0.1:`. If you see `0.0.0.0:`,
stop and fix `BIND_ADDR` before continuing.

---

## 5. Point the bridge at it

Two changes on the cluster. The AES key must match what you put in `.env`:

```bash
kubectl -n pinkline patch secret bridge-secret --type merge -p \
  "{\"data\":{\"SCADA_AES_KEY\":\"$(printf %s '<the key from .env>' | base64 -w0)\"}}"

kubectl -n pinkline patch cm bridge-config --type merge -p \
  '{"data":{"RABBITMQ_HOST":"10.9.0.1","RABBITMQ_PORT":"5672"}}'

kubectl -n pinkline patch secret bridge-secret --type merge -p \
  "{\"data\":{\"RABBITMQ_USER\":\"$(printf %s '<user>' | base64 -w0)\",
              \"RABBITMQ_PASS\":\"$(printf %s '<pass>' | base64 -w0)\"}}"

kubectl -n pinkline rollout restart deploy/pas-scada-bridge
kubectl -n pinkline rollout status  deploy/pas-scada-bridge --timeout=180s
```

Nothing else in the pipeline moves. Artemis, Kafka and the scheduler stay
exactly where they are.

The bridge declares the reverse-path queue and binding itself on every
reconnect, so there is nothing to pre-create on this VM.

---

## 6. Verify end to end

Watch the bridge connect:

```bash
kubectl -n pinkline logs deploy/pas-scada-bridge --tail=50 \
  | grep -iE 'RabbitTopology|Created new connection|→ RabbitMQ'
```

Then send a synthetic message and watch it arrive. On the VM:

```bash
docker compose exec scada-api python -c "
import urllib.request,json
d=json.load(urllib.request.urlopen('http://localhost:8091/api/status'))
print(d['stats'])"
```

`decrypt_ok` climbing means the whole chain works and the keys match.
`decrypt_fail` climbing means the keys differ, or the bridge has
`bridge.encrypt.enabled=false`.

Open the dashboard over an SSH tunnel:

```bash
ssh -L 8091:127.0.0.1:8091 root@<vm-ip>
# then browse http://localhost:8091
```

Trigger the reverse direction from the VM and watch it reach Artemis:

```bash
docker compose exec scada-api python -c "
import urllib.request,json
r=urllib.request.Request('http://localhost:8091/api/publish',
  data=json.dumps({'id':'ESC_ROL_1_23','state':'1'}).encode(),
  headers={'Content-Type':'application/json'})
print(urllib.request.urlopen(r).read().decode())"
```

```bash
kubectl -n pinkline logs deploy/pas-scada-bridge --tail=30 \
  | grep -iE 'scada-source|artemis-sink'
```

---

## Hardening checklist

- [ ] Hetzner firewall allows only UDP 51820, only from the cluster egress IP
- [ ] `ss -tlnp` shows no broker port on `0.0.0.0`
- [ ] `RABBITMQ_USER` / `RABBITMQ_PASS` are not `thiru` / `password`
- [ ] `SCADA_AES_KEY` is newly generated, not the one committed to the repo
- [ ] Management UI reached only by SSH tunnel, never published
- [ ] `.env` is not committed and is `chmod 600`
- [ ] Synthetic data only, no live scheduler traffic

---

## Troubleshooting

**Bridge logs `Connection refused` to 10.9.0.1** — the tunnel is down or the
broker bound to loopback. Check `wg show` for a handshake, then `ss -tlnp`.

**`decrypt_fail` climbing on every message** — the two AES keys differ, or
the bridge is publishing plain. Compare both sides:

```bash
kubectl -n pinkline get secret bridge-secret -o jsonpath='{.data.SCADA_AES_KEY}' | base64 -d; echo
grep SCADA_AES_KEY /opt/external-scada/hetzner/.env
```

**Nothing arrives at all** — the bridge connected but the scheduler is quiet.
Confirm with `kubectl -n pinkline logs deploy/pas-scada-bridge --since=10m |
grep -c 'Artemis'`. A zero means nothing is being published upstream, which
is a TMS-side question, not a VM one.

**MQTT client refused** — `mqtt.allow_anonymous` is `false` here by design
and there is no `default_user`, unlike the lab config. Every client must
present credentials.

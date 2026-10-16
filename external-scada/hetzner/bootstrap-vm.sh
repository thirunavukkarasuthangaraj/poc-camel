#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════
# One-shot bootstrap for the SCADA-side VM.
#
# Run ON THE VM, as root, after copying external-scada there:
#     scp -r external-scada root@<vm-ip>:/opt/pas-scada/
#     ssh root@<vm-ip>
#     cd /opt/pas-scada/hetzner && bash bootstrap-vm.sh
#
# Does: WireGuard (VM side) → firewall → secrets → docker compose up.
# Idempotent: re-running keeps existing keys and secrets.
# ════════════════════════════════════════════════════════════════════
set -euo pipefail

VM_PUBLIC_IP="${VM_PUBLIC_IP:-$(curl -s -4 --max-time 5 ifconfig.me || true)}"
WG_PORT="${WG_PORT:-51820}"
VM_WG_IP="10.9.0.1"
TMS_WG_IP="10.9.0.2"
HERE="$(cd "$(dirname "$0")" && pwd)"

[ "$(id -u)" -eq 0 ] || { echo "run as root"; exit 1; }
[ -n "$VM_PUBLIC_IP" ] || { echo "set VM_PUBLIC_IP=<ip> and re-run"; exit 1; }

echo "→ VM public IP: $VM_PUBLIC_IP"

# ── 1. WireGuard ────────────────────────────────────────────────────
echo "→ installing WireGuard"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq wireguard

install -d -m 700 /etc/wireguard
cd /etc/wireguard

# Keys are generated once and reused on re-run.
[ -f vm_private.key  ] || wg genkey | tee vm_private.key  | wg pubkey > vm_public.key
[ -f tms_private.key ] || wg genkey | tee tms_private.key | wg pubkey > tms_public.key
chmod 600 ./*.key

cat > /etc/wireguard/wg0.conf <<EOF
[Interface]
Address    = ${VM_WG_IP}/24
ListenPort = ${WG_PORT}
PrivateKey = $(cat vm_private.key)

[Peer]
# TMS-side cluster node
PublicKey  = $(cat tms_public.key)
AllowedIPs = ${TMS_WG_IP}/32
EOF
chmod 600 /etc/wireguard/wg0.conf

# The config the TMS node needs. Fetch it FROM the TMS node over your
# own SSH session so the private key never travels anywhere else.
install -d -m 700 /opt/pas-scada
cat > /opt/pas-scada/tms-wg0.conf <<EOF
[Interface]
Address    = ${TMS_WG_IP}/24
PrivateKey = $(cat tms_private.key)

[Peer]
PublicKey           = $(cat vm_public.key)
Endpoint            = ${VM_PUBLIC_IP}:${WG_PORT}
AllowedIPs          = ${VM_WG_IP}/32
PersistentKeepalive = 25
EOF
chmod 600 /opt/pas-scada/tms-wg0.conf

systemctl enable --now wg-quick@wg0 >/dev/null 2>&1 || systemctl restart wg-quick@wg0
sleep 2

# ── 2. Firewall ─────────────────────────────────────────────────────
# WireGuard's port is safe to expose: it never replies to a packet it
# cannot authenticate, so it is invisible to scanners.
if command -v ufw >/dev/null; then
  echo "→ opening ${WG_PORT}/udp"
  ufw allow "${WG_PORT}/udp" >/dev/null
fi

# ── 3. Secrets ──────────────────────────────────────────────────────
cd "$HERE"
if [ ! -f .env ]; then
  echo "→ generating .env with fresh secrets"
  cat > .env <<EOF
BIND_ADDR=${VM_WG_IP}
RABBITMQ_USER=pasbridge
RABBITMQ_PASS=$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)
SCADA_AES_KEY=$(openssl rand -base64 32)
DASHBOARD_BIND=127.0.0.1
ARTEMIS_HOST=
ARTEMIS_USER=
ARTEMIS_PASS=
EOF
  chmod 600 .env
else
  echo "→ .env already exists, leaving it alone"
fi

# ── 4. Up ───────────────────────────────────────────────────────────
echo "→ starting containers"
docker compose up -d --build
sleep 10

# ── 5. Report ───────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════════════════════"
echo " WireGuard"
wg show || true
echo
echo " Listeners — every line must be 10.9.0.1: or 127.0.0.1:"
ss -tlnp 2>/dev/null | grep -E ':(5672|1883|15672|8091)\s' || echo "  (none yet — give it a moment)"
echo
echo " Containers"
docker compose ps
echo
echo "════════════════════════════════════════════════════════════"
echo " NEXT, on the TMS side:"
echo
echo "  1. Copy the peer config to a cluster node and bring it up:"
echo "       scp root@${VM_PUBLIC_IP}:/opt/pas-scada/tms-wg0.conf /etc/wireguard/wg0.conf"
echo "       chmod 600 /etc/wireguard/wg0.conf"
echo "       systemctl enable --now wg-quick@wg0"
echo "       ping -c3 ${VM_WG_IP}"
echo
echo "  2. Point the bridge at the tunnel:"
echo "       kubectl -n pinkline patch cm bridge-config --type merge -p \\"
echo "         '{\"data\":{\"RABBITMQ_HOST\":\"${VM_WG_IP}\",\"RABBITMQ_PORT\":\"5672\"}}'"
echo
echo "  3. Copy these three values into the bridge secret:"
echo "       RABBITMQ_USER = $(grep '^RABBITMQ_USER=' .env | cut -d= -f2-)"
echo "       RABBITMQ_PASS = $(grep '^RABBITMQ_PASS=' .env | cut -d= -f2-)"
echo "       SCADA_AES_KEY = $(grep '^SCADA_AES_KEY=' .env | cut -d= -f2-)"
echo
echo "       kubectl -n pinkline patch secret bridge-secret --type merge -p \\"
echo "         \"{\\\"stringData\\\":{\\\"RABBITMQ_USER\\\":\\\"...\\\",\\\"RABBITMQ_PASS\\\":\\\"...\\\",\\\"SCADA_AES_KEY\\\":\\\"...\\\"}}\""
echo "       kubectl -n pinkline rollout restart deploy/pas-scada-bridge"
echo "════════════════════════════════════════════════════════════"

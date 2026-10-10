#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 4 ]]; then
  echo "Uso: $0 <region> <client-vpn-endpoint-id> <ca-cert.pem> <ca-key.pem>" >&2
  exit 2
fi

region="$1"
endpoint_id="$2"
ca_cert="$(cd "$(dirname "$3")" && pwd)/$(basename "$3")"
ca_key="$(cd "$(dirname "$4")" && pwd)/$(basename "$4")"
output_dir="${VPN_PROFILE_DIR:-$PWD/client-vpn}"

if [[ ! -r "$ca_cert" || ! -r "$ca_key" ]]; then
  echo "No se pueden leer el certificado y la clave de la CA cliente." >&2
  exit 1
fi

mkdir -p "$output_dir"
chmod 700 "$output_dir"

openssl genrsa -out "$output_dir/client.key" 2048
openssl req -new -key "$output_dir/client.key" \
  -out "$output_dir/client.csr" -subj "/CN=inventory-vpn-client"
printf 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n' \
  > "$output_dir/client.ext"
openssl x509 -req -in "$output_dir/client.csr" -CA "$ca_cert" -CAkey "$ca_key" \
  -CAserial "$output_dir/ca.srl" -CAcreateserial -out "$output_dir/client.crt" \
  -days 365 -sha256 -extfile "$output_dir/client.ext"

aws ec2 export-client-vpn-client-configuration \
  --region "$region" \
  --client-vpn-endpoint-id "$endpoint_id" \
  --query ClientConfiguration \
  --output text > "$output_dir/inventory-broker.ovpn"

printf '\n<cert>\n' >> "$output_dir/inventory-broker.ovpn"
cat "$output_dir/client.crt" >> "$output_dir/inventory-broker.ovpn"
printf '</cert>\n<key>\n' >> "$output_dir/inventory-broker.ovpn"
cat "$output_dir/client.key" >> "$output_dir/inventory-broker.ovpn"
printf '</key>\n' >> "$output_dir/inventory-broker.ovpn"
chmod 600 "$output_dir"/client.* "$output_dir/inventory-broker.ovpn"

echo "Perfil VPN creado: $output_dir/inventory-broker.ovpn"
echo "Importa el perfil en AWS VPN Client. No compartas el directorio: contiene claves privadas."
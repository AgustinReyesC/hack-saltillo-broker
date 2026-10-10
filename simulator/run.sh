#!/usr/bin/env bash
# Simulador del taller contra las colas SQS de AWS (inventory-broker-*.fifo).
#
#   simulador                  # 1a vez: manda el dataset; después solo levanta el panel
#   simulador --force-replay   # vuelve a mandar el dataset completo
#   SPEED=0 simulador          # dataset sin pausas (segundos en lugar de ~17 min)
#   simulador --port 8091      # otro puerto para el panel
#
# Usa tus credenciales de AWS (perfil o `aws configure`). El dataset sale del kit del hackathon.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KIT_DIR="${KIT_DIR:-$HOME/HACKSALTILLO/hackathon-inventario-kit}"
export EVENTS="${EVENTS:-$KIT_DIR/data/events.jsonl}"
export EVENT_TRANSPORT=sqs
export AWS_REGION="${AWS_REGION:-us-east-1}"
export API_URL="${API_URL:-http://hack-saltillo-api-850670948.us-east-1.elb.amazonaws.com}"

# Restos de la configuración local (ElasticMQ): con ellos los eventos nunca llegan a AWS.
unset SQS_ENDPOINT_URL SQS_QUEUE_URLS
if [[ "${AWS_ACCESS_KEY_ID:-}" == "test" ]]; then
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
fi

if [[ ! -f "$EVENTS" ]]; then
  echo "No encuentro el dataset en $EVENTS (define KIT_DIR o EVENTS)." >&2
  exit 1
fi

VENV="$HERE/.venv"
if [[ ! -x "$VENV/bin/python" ]] || ! "$VENV/bin/python" -c "import boto3" 2>/dev/null; then
  echo "Preparando el entorno de Python del simulador (solo la primera vez)..."
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet -r "$HERE/requirements.txt"
fi

exec "$VENV/bin/python" "$HERE/simulator.py" --transport sqs "$@"

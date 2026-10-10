# Broker AWS: colas SQS FIFO

Terraform crea seis colas SQS FIFO para los canales del contrato: `shop.catalog`, `shop.work_orders`,
`shop.inspections`, `shop.purchasing`, `inventory.events` e `inventory.dlq`. El simulador puede enviar
eventos desde tu Mac usando el SDK de AWS y sus credenciales locales; no requiere MSK, VPC, VPN ni
certificados. GitHub Actions despliega con OIDC, sin guardar access keys en GitHub.

Cada mensaje incluye el tópico lógico y el sobre del evento. La llave del evento es el grupo FIFO y
`event_id` se usa para la deduplicación de SQS. Los consumidores deben leer su cola, procesar el
mensaje y borrarlo al terminar.

## Límites

La oferta de nivel gratuito de AWS indica hasta 1 millón de solicitudes SQS al mes; verifica sus
condiciones en tu cuenta y región. Cada cola retiene mensajes hasta 14 días. Esto no sustituye la
retención histórica de Kafka de 90 días ni permite reconstruir el dataset de seis semanas solo desde
SQS. Las colas FIFO garantizan el orden dentro de cada grupo de mensajes, no entre colas.

Este cambio tampoco convierte automáticamente consumidores Kafka a SQS. El servicio de inventario y
las pruebas de contrato actuales usan Kafka; para conectar ese servicio a estas colas hay que adaptar
su consumo. El broker Kafka local de Redpanda sigue disponible para desarrollo y pruebas existentes.

## SQS local y Kafka para compatibilidad

El Compose del kit levanta ElasticMQ en `localhost:9324`; el simulador del Compose usa
`http://elasticmq:9324` y crea automáticamente las seis colas FIFO. Para levantarlo desde la raíz del
kit:

```bash
docker compose up -d elasticmq simulator
docker compose logs -f simulator
```

También puedes ejecutar el simulador directamente en tu Mac apuntando al endpoint local:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-west-2 \
SQS_ENDPOINT_URL=http://localhost:9324 EVENT_TRANSPORT=sqs \
simulator/.venv/bin/python simulator/simulator.py --transport sqs
```

Las credenciales `test` solo se usan con el emulador local. Redpanda continúa disponible en
`localhost:19092` para pruebas de contrato y consumidores Kafka existentes. El consumidor de
inventario no se ha migrado todavía a SQS.

## 1. Bootstrap único de Terraform y GitHub OIDC

El bootstrap crea el bucket S3 cifrado/versionado del state, el proveedor OIDC y un rol limitado al
repositorio y rama indicados. Para repositorios GitHub creados después del 15 de julio de 2026,
configura también `github_owner_id` y `github_repository_id`, porque GitHub usa esos IDs en el `sub`
inmutable del token OIDC. Ejecútalo con un perfil local autorizado para crear IAM y S3.

```bash
cp bootstrap/terraform.tfvars.example bootstrap/terraform.tfvars
# Edita aws_region, github_repository (OWNER/REPOSITORY) y github_branch.
terraform -chdir=bootstrap init
terraform -chdir=bootstrap apply
```

Si tu cuenta ya tiene el proveedor OIDC `token.actions.githubusercontent.com`, impórtalo al state del
bootstrap antes de aplicar:

```bash
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
terraform -chdir=bootstrap import aws_iam_openid_connect_provider.github \
  "arn:aws:iam::${ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"
```

En Settings → Secrets and variables → Actions → Variables del repo agrega:

| Variable             | Valor                           |
| -------------------- | ------------------------------- |
| `AWS_ROLE_TO_ASSUME` | Output `github_deploy_role_arn` |
| `AWS_REGION`         | Región AWS para las colas       |
| `TF_STATE_BUCKET`    | Output `terraform_state_bucket` |

El rol del workflow usa `PowerUserAccess`, que es amplio; para producción conviene una política IAM
de mínimo privilegio. La trust policy solo admite el repositorio y rama indicados en el bootstrap.

## 2. Despliegue

El workflow está en `.github/workflows/deploy-broker.yml`. Al subir cambios a `main`, GitHub autentica
por OIDC, valida y aplica Terraform. `workflow_dispatch` permite lanzarlo manualmente. El backend S3
usa la llave `broker/terraform.tfstate`.

Si ya desplegaste la infraestructura MSK anterior con este mismo state, el plan eliminará MSK, VPC,
VPN, KMS y Secrets Manager y creará las colas. Revisa el plan antes de aplicarlo: destruir MSK también
elimina sus datos.

## 3. Enviar eventos del simulador a SQS

Desde la raíz del kit (la carpeta que contiene `simulator/`), instala las dependencias y autentícate
con un perfil que tenga `sqs:SendMessage` en las seis colas:

```bash
python3 -m venv simulator/.venv
simulator/.venv/bin/pip install -r simulator/requirements.txt
export AWS_PROFILE=broker-bootstrap
export AWS_REGION=us-west-2
aws sts get-caller-identity
export TF_STATE_BUCKET="$(terraform -chdir=broker/bootstrap output -raw terraform_state_bucket)"
terraform -chdir=broker init \
  -backend-config="bucket=$TF_STATE_BUCKET" \
  -backend-config="key=broker/terraform.tfstate" \
  -backend-config="region=$AWS_REGION" \
  -backend-config="encrypt=true" \
  -backend-config="use_lockfile=true"
export EVENT_TRANSPORT=sqs
export SQS_QUEUE_URLS="$(terraform -chdir=broker output -json sqs_queue_urls)"
simulator/.venv/bin/python simulator/simulator.py --transport sqs
```

El proceso usa las credenciales de AWS del perfil y envía mensajes en lotes de hasta 10. Para seguir
usando Redpanda local, omite las variables SQS y ejecuta el simulador con el transporte Kafka
predeterminado.

## Destruir recursos

Ejecuta `terraform destroy` con el mismo backend y región. Esto elimina las colas y los mensajes que
aún retengan. El bucket y el state del bootstrap permanecen para conservar el historial de Terraform.

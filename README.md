# Broker AWS: Amazon MSK privado + Client VPN

Esta carpeta contiene la infraestructura del broker en AWS. Crea una VPC privada en dos zonas,
un clúster Amazon MSK provisionado con TLS y SASL/SCRAM, el secreto de SCRAM cifrado con KMS,
y un endpoint AWS Client VPN con autenticación mutua por certificados. No crea la API ni una base
de datos.

El simulador puede permanecer en tu Mac: con la VPN conectada publica directamente a MSK. El
workflow de GitHub Actions aplica Terraform al hacer push a `main`, o manualmente desde `Actions`.
El workflow usa OIDC; no requiere access keys de AWS en GitHub.

## Ejecutar solo el broker local

Desde la raíz de este repositorio independiente:

```bash
docker compose up -d
docker compose ps
```

Redpanda queda en `localhost:19092`; la consola, en http://localhost:8081. Para observar hasta 10
mensajes ya guardados en un tópico:

```bash
docker compose exec -T redpanda rpk topic consume -n 10 -o start shop.catalog
```

Para apagarlo, ejecuta `docker compose down`. Este Compose local inicia solo broker y consola; el
simulador permanece fuera de este repositorio y no publica el dataset automáticamente. Como el
Compose local no monta un volumen, `down` elimina el contenedor y sus mensajes de prueba.

## Costos y límites

MSK Provisioned mantiene dos brokers encendidos y Client VPN factura por endpoint y por conexión.
Hay cargos adicionales por EBS, CloudWatch y transferencia. No es una arquitectura gratuita; revisa
precios de la región antes de aplicar. No hay IP pública para MSK ni NAT Gateway. El bucket remoto de
Terraform no se elimina con `terraform destroy`.

## 1. Certificados Client VPN

Necesitas un certificado de servidor y una CA para firmar certificados de clientes. Importa el
certificado de servidor y la CA en ACM **en la misma región** donde crearás MSK. Conserva la clave de
la CA sin subirla a GitHub. Ejemplo de creación local con OpenSSL:

Configura primero un perfil local con permisos para importar certificados y crear el bootstrap. Las
credenciales permanecen en tu máquina:

```bash
aws configure --profile broker-bootstrap
export AWS_PROFILE=broker-bootstrap
```

```bash
mkdir -p certs
cd certs
openssl genrsa -out ca.key 3072
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
  -out ca.crt -subj "/CN=inventory-client-vpn-ca"

openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/CN=inventory-client-vpn"
printf 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n' > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 365 -sha256 -extfile server.ext

aws acm import-certificate --region us-west-2 \
  --certificate fileb://server.crt --private-key fileb://server.key \
  --certificate-chain fileb://ca.crt
aws acm import-certificate --region us-west-2 \
  --certificate fileb://ca.crt --private-key fileb://ca.key
cd ..
```

Guarda los dos ARN que devuelve ACM. Genera un certificado cliente firmado por `ca.key` para cada
dispositivo/persona que usará la VPN; mantén privados `ca.key`, `server.key` y la clave del cliente.
Los archivos bajo `certs/` están excluidos de Git. Consulta la guía de AWS Client VPN para
exportar el perfil `.ovpn` y añadir el certificado/clave cliente; el perfil también es confidencial.

Una vez desplegado Terraform, el helper siguiente crea una clave y certificado cliente firmados por
tu CA, exporta la configuración del endpoint y empaqueta el perfil para AWS VPN Client:

```bash
bash scripts/create-vpn-profile.sh us-west-2 cvpn-endpoint-0123456789abcdef0 \
  certs/ca.crt certs/ca.key
```

El directorio `client-vpn/` queda excluido de Git. Cada usuario/dispositivo debe tener certificado
propio; ejecuta el script con un `VPN_PROFILE_DIR` diferente para cada perfil.

## 2. Bootstrap único de Terraform y GitHub OIDC

El bootstrap crea el bucket S3 cifrado/versionado del state, el proveedor OIDC y un rol limitado al
repositorio y rama indicados. Ejecútalo **una vez** con un perfil local de AWS autorizado para crear
IAM y S3. Requiere AWS CLI, Terraform y credenciales locales; no imprime ni necesita guardar access
keys en GitHub.

```bash
cp bootstrap/terraform.tfvars.example bootstrap/terraform.tfvars
# Edita aws_region, github_repository (OWNER/REPOSITORY) y github_branch.
terraform -chdir=bootstrap init
terraform -chdir=bootstrap apply
```

Si tu cuenta ya tiene el proveedor OIDC `token.actions.githubusercontent.com`, impórtalo al state del
bootstrap antes de aplicar, en vez de intentar crearlo otra vez:

```bash
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
terraform -chdir=bootstrap import aws_iam_openid_connect_provider.github \
  "arn:aws:iam::${ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"
```

Guarda los outputs:

```bash
terraform -chdir=bootstrap output -raw github_deploy_role_arn
terraform -chdir=bootstrap output -raw terraform_state_bucket
```

En Settings → Secrets and variables → Actions → Variables del repo agrega:

| Variable                          | Valor                                            |
| --------------------------------- | ------------------------------------------------ |
| `AWS_ROLE_TO_ASSUME`              | Output `github_deploy_role_arn`                  |
| `AWS_REGION`                      | La misma región del certificado y del despliegue |
| `TF_STATE_BUCKET`                 | Output `terraform_state_bucket`                  |
| `VPN_SERVER_CERTIFICATE_ARN`      | ARN ACM del certificado de servidor              |
| `VPN_CLIENT_ROOT_CERTIFICATE_ARN` | ARN ACM de la CA cliente                         |

El rol del workflow usa `PowerUserAccess` y permisos acotados para state y service-linked role de
MSK. Para una organización/producción, reemplázalo por una política IAM propia de mínimo privilegio.
La trust policy solo admite el `OWNER/REPOSITORY` y branch del bootstrap. GitHub debe tener este
repositorio conectado a un remoto y la rama configurada debe llamarse igual.

## 3. Despliegue automático

El workflow está en `.github/workflows/deploy-broker.yml`, relativo a esta carpeta, porque vas a
publicar **el contenido de `broker/` como raíz de un repositorio independiente**. Al subir cambios
a `main`, GitHub autentica por OIDC, valida y aplica el plan automáticamente. `workflow_dispatch`
permite lanzarlo manualmente. El workflow usa Terraform 1.10 o posterior y almacena el state/lock en
S3.

El workflow no necesita un archivo `terraform.tfvars`: toma región y ARN desde las variables de
GitHub. Mantén `terraform.tfvars` local si vas a correr Terraform manualmente; está excluido de Git.
Revisa el costo estimado antes de habilitar el workflow en `main`: cada push puede crear o modificar
recursos cobrables.

## 4. Conectar el simulador local

Después del deploy, conecta la aplicación AWS Client VPN con tu certificado cliente. Descarga el
bootstrap de MSK y el ARN del secreto desde el output del workflow o con AWS CLI/estado Terraform.
Con la VPN conectada, los nombres DNS privados de los brokers deben resolver desde tu Mac.

El código del simulador no está en este repo: corre los comandos desde la raíz local del kit (la
carpeta que contiene `simulator/`), con el broker escuchando por la VPN. Instala dependencias y
exporta el usuario/contraseña SCRAM desde Secrets Manager sin imprimir la contraseña:

```bash
python3 -m venv simulator/.venv
simulator/.venv/bin/pip install -r simulator/requirements.txt

export BROKER_BOOTSTRAP="<bootstrap-brokers-sasl-scram de MSK>"
export BROKER_SECRET_ARN="<ARN del output scram_secret_arn>"
export BROKER_SASL_USERNAME="$(aws secretsmanager get-secret-value --secret-id "$BROKER_SECRET_ARN" --query SecretString --output text | jq -r .username)"
export BROKER_SASL_PASSWORD="$(aws secretsmanager get-secret-value --secret-id "$BROKER_SECRET_ARN" --query SecretString --output text | jq -r .password)"
BROKER_SECURITY_PROTOCOL=SASL_SSL \
BROKER_SASL_MECHANISM=SCRAM-SHA-512 \
BROKER_SASL_USERNAME="$BROKER_SASL_USERNAME" \
BROKER_SASL_PASSWORD="$BROKER_SASL_PASSWORD" \
TOPIC_REPLICATION_FACTOR=2 \
simulator/.venv/bin/python simulator/simulator.py --broker "$BROKER_BOOTSTRAP"
```

El simulador crea los tópicos con tres particiones, factor de replicación dos y retención de 90
días, luego publica el dataset. El `docker-compose.yml` raíz sigue siendo para el broker local de
desarrollo; para AWS ejecuta el simulador en tu Mac como arriba y con la VPN conectada. No publiques
el bootstrap privado, el perfil `.ovpn` ni las credenciales SCRAM.

## Destruir recursos

Cuando termines, ejecuta `terraform destroy` usando el mismo backend S3 y variables.
Esto elimina MSK, VPC, VPN, clave KMS y secreto SCRAM; también pierdes los eventos almacenados. El
bucket y el state del bootstrap permanecen para no borrar accidentalmente el historial de
infraestructura. Considera apagar MSK/VPN si no se están usando: ambos tienen costo continuo.

variable "aws_region" {
  description = "Región AWS donde se crearán MSK, VPC y Client VPN."
  type        = string
}

variable "project_name" {
  description = "Prefijo para nombres y etiquetas de recursos."
  type        = string
  default     = "inventory-broker"
}

variable "cluster_name" {
  description = "Nombre del clúster Kafka de Amazon MSK."
  type        = string
  default     = "inventory-events"
}

variable "kafka_version" {
  description = "Versión de Kafka soportada por Amazon MSK en la región elegida."
  type        = string
  default     = "3.8.0"
}

variable "broker_instance_type" {
  description = "Tipo de instancia por broker MSK."
  type        = string
  default     = "kafka.t3.small"
}

variable "ebs_volume_size" {
  description = "Almacenamiento EBS en GiB por broker."
  type        = number
  default     = 20
}

variable "vpc_cidr" {
  description = "CIDR privado de la VPC del broker."
  type        = string
  default     = "10.40.0.0/16"
}

variable "client_vpn_cidr" {
  description = "CIDR exclusivo para clientes VPN; no debe solaparse con la VPC ni la red local."
  type        = string
  default     = "172.28.0.0/22"
}

variable "vpn_server_certificate_arn" {
  description = "ARN en ACM del certificado de servidor de AWS Client VPN."
  type        = string
}

variable "vpn_client_root_certificate_arn" {
  description = "ARN en ACM del certificado raíz que firmó los certificados de cliente VPN."
  type        = string
}

variable "scram_username" {
  description = "Usuario SCRAM usado por el simulador y consumidores autorizados."
  type        = string
  default     = "inventory-simulator"
}

variable "application_security_group_ids" {
  description = "Security Groups de servicios dentro de la VPC autorizados a consumir MSK en TLS/SCRAM."
  type        = set(string)
  default     = []
}

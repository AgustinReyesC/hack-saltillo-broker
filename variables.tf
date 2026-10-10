variable "aws_region" {
  description = "Región AWS donde se crearán las colas SQS."
  type        = string
}

variable "project_name" {
  description = "Prefijo para nombres y etiquetas de recursos."
  type        = string
  default     = "inventory-broker"
}

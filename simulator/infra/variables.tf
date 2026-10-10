variable "aws_region" {
  description = "Región de las colas, el cluster y el ALB."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Cluster ECS donde corre la API (se comparte)."
  type        = string
  default     = "hack-saltillo-api"
}

variable "load_balancer_name" {
  description = "ALB de la API; el panel entra por la regla /simulador*."
  type        = string
  default     = "hack-saltillo-api"
}

variable "image_tag" {
  description = "Tag de la imagen del simulador en ECR."
  type        = string
}

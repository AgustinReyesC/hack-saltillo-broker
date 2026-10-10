variable "aws_region" {
  description = "Región donde se guardará el state remoto y se usará el rol de deploy."
  type        = string
}

variable "github_repository" {
  description = "Repositorio GitHub en formato OWNER/REPOSITORY."
  type        = string
}

variable "github_branch" {
  description = "Rama autorizada a asumir el rol OIDC."
  type        = string
  default     = "main"
}

variable "state_bucket_name" {
  description = "Nombre opcional del bucket globalmente único para el state."
  type        = string
  default     = ""
}

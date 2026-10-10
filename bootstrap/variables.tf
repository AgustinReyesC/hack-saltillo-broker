variable "aws_region" {
  description = "Región donde se guardará el state remoto y se usará el rol de deploy."
  type        = string
}

variable "github_repository" {
  description = "Repositorio GitHub en formato OWNER/REPOSITORY."
  type        = string
}

variable "github_owner_id" {
  description = "ID numérico inmutable del propietario GitHub; requerido para repositorios con subject OIDC inmutable."
  type        = string
  default     = ""
}

variable "github_repository_id" {
  description = "ID numérico inmutable del repositorio GitHub; requerido para repositorios con subject OIDC inmutable."
  type        = string
  default     = ""

  validation {
    condition     = (var.github_owner_id == "" && var.github_repository_id == "") || (var.github_owner_id != "" && var.github_repository_id != "")
    error_message = "Configura juntos github_owner_id y github_repository_id, o deja ambos vacíos para el formato OIDC anterior."
  }
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

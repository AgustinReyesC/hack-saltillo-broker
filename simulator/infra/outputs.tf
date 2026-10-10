output "ecr_repository_url" {
  value = aws_ecr_repository.simulator.repository_url
}

output "panel_password_secret" {
  description = "Secreto con la contraseña del panel."
  value       = aws_secretsmanager_secret.panel.name
}

output "alb_path" {
  description = "Ruta del panel en el ALB (CloudFront la publica por HTTPS)."
  value       = "http://${data.aws_lb.api.dns_name}/simulador/"
}

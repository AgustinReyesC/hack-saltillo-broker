output "msk_cluster_arn" {
  description = "ARN del clúster Amazon MSK."
  value       = aws_msk_cluster.broker.arn
}

output "bootstrap_brokers_sasl_scram_tls" {
  description = "Bootstrap brokers privados; solo accesibles desde la VPC o la Client VPN."
  value       = aws_msk_cluster.broker.bootstrap_brokers_sasl_scram
}

output "scram_secret_arn" {
  description = "ARN del secreto SCRAM en Secrets Manager (no contiene la contraseña en el output)."
  value       = aws_secretsmanager_secret.scram.arn
}

output "client_vpn_endpoint_id" {
  description = "ID del endpoint AWS Client VPN."
  value       = aws_ec2_client_vpn_endpoint.broker.id
}

output "vpc_id" {
  description = "ID de la VPC privada del broker."
  value       = aws_vpc.broker.id
}

output "private_subnet_ids" {
  description = "IDs de las subredes privadas multi-AZ."
  value       = [for subnet in aws_subnet.private : subnet.id]
}

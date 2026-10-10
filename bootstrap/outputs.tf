output "github_deploy_role_arn" {
  description = "ARN para el secret/variable AWS_ROLE_TO_ASSUME de GitHub Actions."
  value       = aws_iam_role.github_deploy.arn
}

output "terraform_state_bucket" {
  description = "Bucket cifrado/versionado para el backend S3 del broker."
  value       = aws_s3_bucket.terraform_state.id
}

output "github_oidc_provider_arn" {
  description = "Proveedor OIDC de GitHub creado en IAM."
  value       = aws_iam_openid_connect_provider.github.arn
}
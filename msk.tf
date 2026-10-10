resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.cluster_name}"
  retention_in_days = 14
}

resource "aws_kms_key" "scram" {
  description             = "Cifrado del secreto SCRAM de ${var.cluster_name}"
  enable_key_rotation     = true
  deletion_window_in_days = 7

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableAccountAdministration"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowMSKToDecryptSCRAMSecret"
        Effect    = "Allow"
        Principal = { Service = "kafka.amazonaws.com" }
        Action    = ["kms:Decrypt", "kms:DescribeKey"]
        Resource  = "*"
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
          ArnLike      = { "aws:SourceArn" = aws_msk_cluster.broker.arn }
        }
      }
    ]
  })
}

resource "aws_kms_alias" "scram" {
  name          = "alias/${var.project_name}-scram"
  target_key_id = aws_kms_key.scram.key_id
}

resource "aws_msk_cluster" "broker" {
  cluster_name           = var.cluster_name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = length(local.availability_zones)
  enhanced_monitoring    = "DEFAULT"

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = [for subnet in aws_subnet.private : subnet.id]
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size
      }
    }
  }

  client_authentication {
    sasl {
      scram = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

}

resource "random_password" "scram" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "scram" {
  name                    = "AmazonMSK_${var.cluster_name}_scram"
  description             = "Credencial SCRAM para el broker ${var.cluster_name}"
  kms_key_id              = aws_kms_key.scram.arn
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "scram" {
  secret_id = aws_secretsmanager_secret.scram.id
  secret_string = jsonencode({
    username = var.scram_username
    password = random_password.scram.result
  })
}

resource "aws_secretsmanager_secret_policy" "scram" {
  secret_arn = aws_secretsmanager_secret.scram.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowMSKReadSecret"
      Effect    = "Allow"
      Principal = { Service = "kafka.amazonaws.com" }
      Action    = "secretsmanager:GetSecretValue"
      Resource  = aws_secretsmanager_secret.scram.arn
      Condition = {
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
        ArnLike      = { "aws:SourceArn" = aws_msk_cluster.broker.arn }
      }
    }]
  })
}

resource "aws_msk_scram_secret_association" "scram" {
  cluster_arn     = aws_msk_cluster.broker.arn
  secret_arn_list = [aws_secretsmanager_secret.scram.arn]

  depends_on = [aws_secretsmanager_secret_policy.scram]
}

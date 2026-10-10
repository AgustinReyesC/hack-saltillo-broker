resource "aws_vpc" "broker" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

resource "aws_subnet" "private" {
  for_each = local.private_subnets

  vpc_id                  = aws_vpc.broker.id
  availability_zone       = each.key
  cidr_block              = each.value
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.project_name}-private-${each.key}"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.broker.id

  tags = {
    Name = "${var.project_name}-private"
  }
}

resource "aws_route_table_association" "private" {
  for_each = aws_subnet.private

  subnet_id      = each.value.id
  route_table_id = aws_route_table.private.id
}

resource "aws_security_group" "client_vpn" {
  name        = "${var.project_name}-client-vpn"
  description = "Acceso de clientes VPN al VPC privado del broker"
  vpc_id      = aws_vpc.broker.id

  egress {
    description = "Permitir tráfico hacia la VPC; el SG de MSK limita el destino a Kafka TLS"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }
}

resource "aws_security_group" "msk" {
  name        = "${var.project_name}-msk"
  description = "Acceso al listener SCRAM/TLS y tráfico interno entre brokers"
  vpc_id      = aws_vpc.broker.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_vpn" {
  security_group_id = aws_security_group.msk.id
  cidr_ipv4         = var.client_vpn_cidr
  description       = "Kafka TLS/SCRAM desde Client VPN"
  from_port         = 9096
  to_port           = 9096
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_app" {
  for_each = var.application_security_group_ids

  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = each.value
  description                  = "Kafka TLS/SCRAM desde servicio de aplicación en la VPC"
  from_port                    = 9096
  to_port                      = 9096
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "broker_replication" {
  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = aws_security_group.msk.id
  description                  = "Replicación TLS entre brokers MSK"
  from_port                    = 9094
  to_port                      = 9094
  ip_protocol                  = "tcp"
}

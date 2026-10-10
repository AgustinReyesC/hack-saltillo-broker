resource "aws_ec2_client_vpn_endpoint" "broker" {
  description            = "Client VPN privada para el broker ${var.cluster_name}"
  server_certificate_arn = var.vpn_server_certificate_arn
  client_cidr_block      = var.client_vpn_cidr
  split_tunnel           = true
  transport_protocol     = "udp"
  vpn_port               = 443
  dns_servers            = [cidrhost(var.vpc_cidr, 2)]
  security_group_ids     = [aws_security_group.client_vpn.id]
  vpc_id                 = aws_vpc.broker.id

  authentication_options {
    type                       = "certificate-authentication"
    root_certificate_chain_arn = var.vpn_client_root_certificate_arn
  }

  connection_log_options {
    enabled = false
  }

  tags = {
    Name = "${var.project_name}-client-vpn"
  }
}

resource "aws_ec2_client_vpn_network_association" "private" {
  for_each = aws_subnet.private

  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.broker.id
  subnet_id              = each.value.id
}

resource "aws_ec2_client_vpn_authorization_rule" "vpc" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.broker.id
  target_network_cidr    = var.vpc_cidr
  authorize_all_groups   = true
  description            = "Acceso de clientes con certificado a la VPC privada del broker"
}

resource "aws_ec2_client_vpn_route" "vpc" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.broker.id
  destination_cidr_block = var.vpc_cidr
  target_vpc_subnet_id   = aws_subnet.private[local.availability_zones[0]].id

  depends_on = [aws_ec2_client_vpn_network_association.private]
}

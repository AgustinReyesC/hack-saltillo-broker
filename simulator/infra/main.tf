# Panel del simulador en ECS Fargate, en el cluster y el ALB de hack-saltillo-api.
# CloudFront (repo hack-saltillo-ui) manda /simulador* al ALB y el ALB a esta tarea.
#
#   terraform -chdir=simulator/infra init -backend-config=backend.hcl
#   terraform -chdir=simulator/infra apply -var image_tag=<tag>

terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "s3" {}
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "hack-saltillo-simulator"
      ManagedBy = "Terraform"
    }
  }
}

locals {
  name       = "hack-saltillo-simulator"
  base_path  = "/simulador"
  state_path = "/hack-saltillo/simulator/state"
}

data "aws_caller_identity" "current" {}

data "aws_ecs_cluster" "api" {
  cluster_name = var.cluster_name
}

data "aws_lb" "api" {
  name = var.load_balancer_name
}

data "aws_lb_listener" "http" {
  load_balancer_arn = data.aws_lb.api.arn
  port              = 80
}

data "aws_security_group" "load_balancer" {
  name   = "${var.load_balancer_name}-alb"
  vpc_id = data.aws_lb.api.vpc_id
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_lb.api.vpc_id]
  }

  filter {
    name   = "map-public-ip-on-launch"
    values = ["true"]
  }
}

resource "aws_ecr_repository" "simulator" {
  name                 = local.name
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_cloudwatch_log_group" "simulator" {
  name              = "/ecs/${local.name}"
  retention_in_days = 14
}

# Contraseña compartida del panel (Basic Auth). Se lee con:
#   aws secretsmanager get-secret-value --secret-id hack-saltillo-simulator/panel-password --query SecretString --output text
resource "random_password" "panel" {
  length  = 20
  special = false
}

resource "aws_secretsmanager_secret" "panel" {
  name                    = "${local.name}/panel-password"
  description             = "Contraseña del panel del simulador (${local.base_path})"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "panel" {
  secret_id     = aws_secretsmanager_secret.panel.id
  secret_string = random_password.panel.result
}

data "aws_iam_policy_document" "ecs_tasks" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "execution_secret" {
  name = "read-panel-password"
  role = aws_iam_role.execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = aws_secretsmanager_secret.panel.arn
    }]
  })
}

# La tarea solo puede publicar en las colas del broker y guardar el avance del dataset.
resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks.json
}

resource "aws_iam_role_policy" "task" {
  name = "publish-shop-events"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:GetQueueUrl", "sqs:SendMessage"]
        Resource = "arn:aws:sqs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:inventory-broker-*"
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:PutParameter"]
        Resource = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter${local.state_path}"
      },
    ]
  })
}

resource "aws_security_group" "task" {
  name        = "${local.name}-task"
  description = "Panel del simulador: solo desde el ALB"
  vpc_id      = data.aws_lb.api.vpc_id

  ingress {
    from_port       = 8090
    to_port         = 8090
    protocol        = "tcp"
    security_groups = [data.aws_security_group.load_balancer.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_lb_target_group" "simulator" {
  name        = local.name
  port        = 8090
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = data.aws_lb.api.vpc_id

  health_check {
    path                = "${local.base_path}/health"
    matcher             = "200"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener_rule" "simulator" {
  listener_arn = data.aws_lb_listener.http.arn
  priority     = 10

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.simulator.arn
  }

  condition {
    path_pattern {
      values = [local.base_path, "${local.base_path}/*"]
    }
  }
}

resource "aws_ecs_task_definition" "simulator" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([{
    name         = local.name
    image        = "${aws_ecr_repository.simulator.repository_url}:${var.image_tag}"
    essential    = true
    portMappings = [{ containerPort = 8090, protocol = "tcp" }]
    environment = [
      { name = "AWS_REGION", value = var.aws_region },
      { name = "BASE_PATH", value = local.base_path },
      { name = "API_URL", value = "http://${data.aws_lb.api.dns_name}" },
      { name = "SIM_STATE_PARAM", value = local.state_path },
    ]
    secrets = [{ name = "PANEL_PASSWORD", valueFrom = aws_secretsmanager_secret.panel.arn }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.simulator.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "simulator"
      }
    }
  }])

  depends_on = [aws_secretsmanager_secret_version.panel]
}

resource "aws_ecs_service" "simulator" {
  name            = local.name
  cluster         = data.aws_ecs_cluster.api.arn
  task_definition = aws_ecs_task_definition.simulator.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  # Una sola tarea: las órdenes del panel viven en su memoria. Al desplegar, la vieja se detiene
  # antes de que arranque la nueva para no tener dos paneles con estados distintos.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  network_configuration {
    subnets          = data.aws_subnets.public.ids
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.simulator.arn
    container_name   = local.name
    container_port   = 8090
  }

  depends_on = [aws_lb_listener_rule.simulator]
}

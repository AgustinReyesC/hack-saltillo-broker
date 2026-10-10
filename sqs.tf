locals {
  event_queues = {
    "shop-catalog"     = "shop.catalog"
    "shop-work-orders" = "shop.work_orders"
    "shop-inspections" = "shop.inspections"
    "shop-purchasing"  = "shop.purchasing"
    "inventory-events" = "inventory.events"
    "inventory-dlq"    = "inventory.dlq"
  }
}

resource "aws_sqs_queue" "events" {
  for_each = local.event_queues

  name                        = "${var.project_name}-${each.key}.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  visibility_timeout_seconds  = 60
  message_retention_seconds   = 1209600
  receive_wait_time_seconds   = 20
  sqs_managed_sse_enabled     = true

  tags = {
    Topic = each.value
  }
}
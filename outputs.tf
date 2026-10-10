output "sqs_queue_urls" {
  description = "URLs de las colas FIFO, indexadas por el nombre lógico del tópico."
  value = {
    for key, queue in aws_sqs_queue.events : local.event_queues[key] => queue.url
  }
}

output "sqs_queue_arns" {
  description = "ARN de las colas FIFO, indexados por el nombre lógico del tópico."
  value = {
    for key, queue in aws_sqs_queue.events : local.event_queues[key] => queue.arn
  }
}

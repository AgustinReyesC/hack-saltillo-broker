# Panel del simulador para ECS (detrás de CloudFront en /simulador/).
# El dataset no se versiona (este repo es público); se pasa al construir:
#   docker buildx build --build-context kit=$HOME/HACKSALTILLO/hackathon-inventario-kit simulator
FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator.py panel.html ./
COPY --from=kit data/events.jsonl /app/data/events.jsonl

ENV EVENTS=/app/data/events.jsonl \
    EVENT_TRANSPORT=sqs \
    PORT=8090 \
    BASE_PATH=/simulador \
    PYTHONUNBUFFERED=1
EXPOSE 8090
CMD ["python", "simulator.py"]

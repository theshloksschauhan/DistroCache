FROM python:3.11-slim

WORKDIR /app

COPY node/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY proto /app/proto
COPY scripts /app/scripts
COPY node /app/node

RUN python /app/scripts/gen_proto.py

EXPOSE 50051
CMD ["python", "-m", "node.app"]

# The Python program (import, workload, indexes); code and data are mounted
# at runtime by docker-compose.yml, so only dependencies are baked in.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

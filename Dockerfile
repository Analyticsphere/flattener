FROM python:3.11-slim-bookworm
ENV PYTHONHASHSEED=0

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

ENV FLASK_APP=/app/core/endpoints.py
ENV FLASK_RUN_HOST=0.0.0.0
 
EXPOSE 8080

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 core.endpoints:app
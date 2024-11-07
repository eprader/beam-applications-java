FROM python:3.9-slim

WORKDIR /app

COPY src/ /app/src/
COPY config_frameworks /app/config_frameworks/
COPY requirements.txt /app/


RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "/app/src/main.py"]
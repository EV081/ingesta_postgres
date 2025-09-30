FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ingesta.py /app/ingesta.py
RUN mkdir -p /app/out

CMD ["python", "/app/ingesta.py"]

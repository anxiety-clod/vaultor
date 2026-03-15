FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN pwd && ls -la

ENTRYPOINT ["python", "bot11.py"]

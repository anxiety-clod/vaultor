FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-c", "import time; print('✅ BOT STARTED'); time.sleep(9999)"]

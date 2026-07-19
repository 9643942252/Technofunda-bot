FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY technofunda_bot.py .

# Railway injects $PORT at runtime; the bot reads it via os.getenv("PORT")
EXPOSE 8080

CMD ["python3", "technofunda_bot.py"]

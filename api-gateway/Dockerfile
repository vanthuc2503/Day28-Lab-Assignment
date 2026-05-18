FROM python:3.10-slim

WORKDIR /app

RUN pip install "fastapi>=0.100" uvicorn "httpx<0.28" prometheus-fastapi-instrumentator langsmith

COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

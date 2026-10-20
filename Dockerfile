FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gateway.py .
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
EXPOSE 8890
CMD ["uvicorn", "gateway:app", "--host", "0.0.0.0", "--port", "8890", "--no-access-log", "--no-proxy-headers", "--timeout-graceful-shutdown", "300"]

FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY WM_WS_E.py .

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "WM_WS_E.py"]

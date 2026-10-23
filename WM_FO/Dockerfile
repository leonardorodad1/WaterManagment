FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY WM_FO.py .

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "WM_FO.py"]
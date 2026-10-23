FROM python:3.12-slim

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /data
COPY WM_CENTRAL.py /app/WM_CENTRAL.py

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/WM_CENTRAL.py"]
CMD ["5000", "broker", "29092"]
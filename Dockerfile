FROM python:3.12-alpine

WORKDIR /app

RUN pip install --no-cache-dir requests>=2.31.0

COPY main.py /app/main.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]

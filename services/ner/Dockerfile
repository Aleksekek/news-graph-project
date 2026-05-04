FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY services/ner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Прогреваем модели Natasha при сборке образа (скачиваются ~50MB, кешируются в слое)
RUN python -c "\
from natasha import NewsEmbedding, NewsMorphTagger, NewsNERTagger; \
emb = NewsEmbedding(); NewsMorphTagger(emb); NewsNERTagger(emb); \
print('Natasha models ready')"

COPY src /app/src
COPY services/ner/app.py .
COPY services/healthcheck.py /app/healthcheck.py

RUN mkdir -p /app/logs

HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD python /app/healthcheck.py || exit 1

CMD ["python", "app.py"]

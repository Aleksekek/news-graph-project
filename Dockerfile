FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -e .

COPY . .

CMD ["python", "-m", "src.domain.scheduling.runner"]

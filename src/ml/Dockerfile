FROM python:3.9-slim

WORKDIR /app

# dependențe sistem necesare pentru psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# dependențe Python
RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    scikit-learn \
    numpy

COPY . /app

CMD ["sleep", "infinity"]

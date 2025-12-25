# =========================
# Pipeline Docker Image
# =========================

# 1. Use lightweight Python image
FROM python:3.10-slim

# 2. Set environment variables
ENV PYTHONUNBUFFERED=1
ENV APP_HOME=/app

# 3. Set working directory
WORKDIR $APP_HOME

# 4. Install system dependencies for PostgreSQL
RUN apt-get update \
    && apt-get install -y gcc libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 5. Copy dependency file first (for caching)
COPY requirements.txt .

# 6. Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 7. Copy application code
COPY . .

# 8. Default command (placeholder for now)
CMD ["sleep", "infinity"]

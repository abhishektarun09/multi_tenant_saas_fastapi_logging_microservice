FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-cache

# Copy project
COPY . .

CMD uv run uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
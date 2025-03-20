FROM ghcr.io/astral-sh/uv:0.6.6-python3.12-bookworm

# Copy the project into the image
ADD . /app

# Sync the project into a new environment, using the frozen lockfile
WORKDIR /app
RUN uv sync --frozen

# Set environment for MCP communication
ENV PYTHONUNBUFFERED=1

# Install package with UV (using --system flag)
RUN uv pip install --system -e .

CMD ["uv", "run", "main.py"]
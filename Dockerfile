# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

#Copy the project files into the image
COPY requirements.txt .

# Install the project dependencies
RUN uv pip install --system -r requirements.txt
COPY . .
# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT ["python3", "-m", "src.runner"]
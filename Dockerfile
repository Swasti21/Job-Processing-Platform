FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY static ./static

# One image, two roles: the API runs uvicorn (default), the worker overrides the command.
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]

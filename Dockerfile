FROM python:3.10.6-slim

ENV PYTHONUNBUFFERED 1

EXPOSE 5789
WORKDIR /app
COPY . /app

RUN pip install --upgrade pip

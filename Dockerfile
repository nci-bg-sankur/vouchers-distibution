FROM python:3.9

ENV PYTHONUNBUFFERED 1
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y --allow-unauthenticated \
        build-essential \
        vim \
        locales \
        cron \
        getttext && \
    apt-get clean

RUN pip install --upgrade pip

WORKDIR /app

COPY . /app/
RUN pip install -r requirements.txt

CMD python distributor.py

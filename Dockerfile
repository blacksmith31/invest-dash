FROM python:3.11

RUN apt-get update && apt-get -y install cron

WORKDIR /invest_dash

COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab && crontab /etc/cron.d/crontab

COPY ./requirements.txt /invest_dash

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . /invest_dash

ENV PYTHONPATH "${PYTHONPATH}:/invest_dash"

RUN /usr/bin/crontab /etc/cron.d/crontab

CMD ["cron", "-f"]
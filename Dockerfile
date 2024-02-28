FROM python:3.11

RUN apt-get update -y
RUN apt-get install -y tzdata
ENV TZ=America/New_York

EXPOSE 9090

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN mkdir /invest_dash
WORKDIR /invest_dash

ENV PYTHONPATH "${PYTHONPATH}:/invest_dash"

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
VOLUME ["/invest_dash/data"]

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9090"]

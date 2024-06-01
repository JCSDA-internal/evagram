FROM python:latest

WORKDIR /backend

COPY requirements.txt .
COPY loaddata.py .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

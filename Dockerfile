FROM python:3.7-slim

COPY main.py /
COPY credentials.json /
WORKDIR /app
ADD  . /app
RUN pip install -r requirements.txt

CMD ["python",  "-u", "main.py"]
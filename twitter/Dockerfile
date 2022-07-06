FROM python:3.10

COPY requirements.txt /
RUN pip3 install -r /requirements.txt

COPY twitter.py /
ENTRYPOINT ["python3", "/twitter.py"]

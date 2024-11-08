#from https://github.com/dockersamples/helloworld-demo-python

FROM python:3.10

#preparare working directory
RUN mkdir /app
WORKDIR /app

ADD . /app

#install dependencies
RUN pip install --progress-bar off -r requirements.txt

CMD ["python3", "./tests/csv/run_tests.py"]

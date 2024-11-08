#from https://github.com/dockersamples/helloworld-demo-python

FROM python:3.10

#preparare working directory
RUN mkdir /app
WORKDIR /app

ADD . /app

ENV PIP_ROOT_USER_ACTION=ignore
#install dependencies
RUN pip install --progress-bar off --root-user-action=ignore   -r requirements.txt

CMD ["python3", "./tests/csv/run_tests.py"]

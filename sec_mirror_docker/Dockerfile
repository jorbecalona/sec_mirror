FROM python:3-alpine

RUN mkdir /code
WORKDIR /code
ADD . /code/

RUN pip install -r requirements.txt

COPY ./skel/* ./
CMD [ "python", "/code/app.py"]
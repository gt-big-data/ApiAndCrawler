FROM python:3

WORKDIR /usr/src/app
MAINTAINER GT Big Data <contact@gtbigdata.club>

COPY . .

RUN pip install -r requirements.txt

CMD [ "python", "./api/api.py" ]

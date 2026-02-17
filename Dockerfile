FROM python:3.9 

WORKDIR reprec
COPY requirements.txt requirements.txt
COPY . .
RUN chmod +x reprec
RUN chmod +x run_tests.sh
RUN pip3 install -r requirements.txt
ENTRYPOINT [ "./reprec" ]

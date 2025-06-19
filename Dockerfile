FROM apache/airflow:2.7.0

USER root
COPY create_user.sh /create_user.sh
COPY requirements.txt /requirements.txt
RUN chmod +x /create_user.sh

USER airflow
RUN pip install -r /requirements.txt

ENTRYPOINT ["/create_user.sh"]
CMD ["airflow", "webserver"]


FROM python

WORKDIR /app
COPY pipeline.py pipeline_c.py

RUN pip install pandas



ENTRYPOINT [ "python", "pipeline_c.py" ]
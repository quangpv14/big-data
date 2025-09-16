FROM bde2020/spark-submit:3.3.0-hadoop3.3
LABEL maintainer="quang <quang.email@gmail.com>"

# Create /app directory in the container
RUN mkdir /app

# Copy ./app/submit.sh
COPY ./submit.sh /

RUN echo "Build Spark application Docker image!"

# Copy all files from ./app in local to /app in container
COPY ./save_elasticsearch.py ./app.py ./hdfs_to_es.py ./requirements.txt /app/

# Set the working directory to /app
WORKDIR /app
RUN pip3 install -r requirements.txt

# Command to run when the container starts
CMD ["sh", "/submit.sh"]
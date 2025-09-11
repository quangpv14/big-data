FROM bde2020/spark-submit:3.3.0-hadoop3.3
LABEL maintainer="quang <quang.email@gmail.com>"

# Create /app directory in the container
RUN mkdir /app
RUN rm /submit.sh
COPY ./submit.sh /

RUN echo "Build number: 1"
# Copy ./app/template.sh
COPY ./template.sh /

# Copy all files from ./app in local to /app in container
#COPY ./app.py ./hadoop_to_elastic.py ./requirements.txt /app/
COPY ./app.py ./requirements.txt /app/

# Set the working directory to /app
WORKDIR /app
RUN pip3 install -r requirements.txt

# Command to run when the container starts
# CMD ["sh", "/submit.sh"]
CMD ["/bin/bash", "/template.sh"]
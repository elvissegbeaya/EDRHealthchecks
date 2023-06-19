# Use an official Python runtime as the base image
FROM python:3

# Install cron
RUN apt-get update && apt-get install -y cron

# Set the working directory in the container
WORKDIR /app

# Copy the Python script to the container
COPY "./EDRAutomationTesting.py" .
COPY "./SampleHelper.py" .
COPY "./welldata.cfg" .
COPY "./welldataAPI.py" .
COPY "./EmailModule.py" .

# Install any required dependencies
RUN pip install pandas tenacity requests pydantic openpyxl sseclient retry schedule

# Add crontab file in the cron directory
ADD crontab /etc/cron.d/hello-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/hello-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log

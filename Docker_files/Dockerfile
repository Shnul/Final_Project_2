# filepath: /c:/Final_Project/Dockerfile
FROM python:3.8-slim

# Install cron
RUN apt-get update && apt-get install -y cron

# Copy the scripts into the container
COPY API_exchange_rate_topic.py /home/developer/Final_Project/API_exchange_rate_topic.py
COPY kafka_to_postgres.py /home/developer/Final_Project/kafka_to_postgres.py
COPY run_exchange_rate.sh /home/developer/Final_Project/run_exchange_rate.sh

# Make the shell script executable
RUN chmod +x /home/developer/Final_Project/run_exchange_rate.sh

# Add the cron jobs
RUN echo "@reboot /usr/bin/python3 /home/developer/Final_Project/kafka_to_postgres.py >> /home/developer/kafka_to_postgres.log 2>&1" >> /etc/cron.d/my-cron-job
RUN echo "@reboot /home/developer/Final_Project/run_exchange_rate.sh >> /home/developer/exchange_rate.log 2>&1" >> /etc/cron.d/my-cron-job
RUN echo "@reboot /usr/bin/python3 /home/developer/Final_Project/kafka_to_postgres.py >> /home/developer/kafka_to_postgres.log 2>&1" >> /etc/cron.d/my-cron-job

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/my-cron-job

# Apply cron job
RUN crontab /etc/cron.d/my-cron-job

# Create the log files to be able to run tail
RUN touch /home/developer/kafka_to_postgres.log /home/developer/exchange_rate.log

# Run the command on container startup
CMD cron && tail -f /home/developer/kafka_to_postgres.log /home/developer/exchange_rate.log

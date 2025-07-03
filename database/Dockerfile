FROM postgres:15

# Set environment variables
ENV POSTGRES_USER=your_user
ENV POSTGRES_PASSWORD=your_password
ENV POSTGRES_DB=mydatabase

# Optionally copy your init scripts or config
COPY ./init.sql /docker-entrypoint-initdb.d/

EXPOSE 5432

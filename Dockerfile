FROM python:3.8

# Needed for something...
ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip
RUN pip install pipenv

#Next section is for installing the ODBC driver for accessing the SQL Server DART DB
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

#Debian 10
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN exit
RUN apt-get -y update
RUN ACCEPT_EULA=Y apt-get -y install msodbcsql17
# optional: for bcp and sqlcmd
RUN ACCEPT_EULA=Y apt-get -y install mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
# RUN source ~/.bashrc
# optional: for unixODBC development headers
RUN apt-get -y install unixodbc-dev
# optional: kerberos library for debian-slim distributions
# RUN apt-get -y install libgssapi-krb5-2

WORKDIR /code

COPY Pipfile /code/
COPY Pipfile.lock /code/

# Install both default and dev packages so that we can run the tests against this image
RUN pipenv install --dev --ignore-pipfile --system --deploy

ADD . /code/

CMD ["python", "runner.py", "--sftp", "--scheduled", "--add-to-dart"]

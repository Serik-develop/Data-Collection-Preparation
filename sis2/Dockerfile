FROM apache/airflow:2.8.1-python3.11

USER root

# Установка Chrome и зависимостей
RUN apt-get update && apt-get install -y wget unzip gnupg2 curl \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Копируем проект внутрь контейнера
COPY . /opt/airflow/

# Установка зависимостей Python
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt webdriver-manager selenium pandas

USER airflow
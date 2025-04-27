# Базовый образ: Perl + Debian
FROM perl:latest

# Обновляем систему и ставим нужное
RUN apt-get update && apt-get install -y \
    redis-server \
    redis-tools \
    vim \
    git \
    curl \
 && apt-get clean

# Устанавливаем Perl-пакеты менеджера
RUN cpanm Carton

# Рабочая папка
WORKDIR /workspace

# Открыть порт для Redis (если вдруг захотим коннектиться снаружи)
EXPOSE 6379

RUN apt-get update && apt-get install -y locales && \
    sed -i 's/^# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# Стартовать Redis при запуске контейнера
CMD service redis-server start && bash
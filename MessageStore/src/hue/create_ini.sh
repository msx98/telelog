#!/bin/bash

# We have /root/.env
source /root/.env

# Create the ini file
echo "[desktop]" > /root/hue.ini

echo "[postgresql]" > /root/hue.ini
echo "engine=postgresql_psycopg2" >> /root/hue.ini
echo "host=${POSTGRES_HOST}" >> /root/hue.ini
echo "port=${POSTGRES_PORT}" >> /root/hue.ini
echo "user=${POSTGRES_USER}" >> /root/hue.ini
echo "password=${POSTGRES_PASSWORD}" >> /root/hue.ini
echo "name=postgres" >> /root/hue.ini

#echo "[mysql]" >> /root/hue.ini
#echo "engine=mysql" >> /root/hue.ini
#echo "host=${MYSQL_HOST}" >> /root/hue.ini
#echo "port=${MYSQL_PORT}" >> /root/hue.ini
#echo "user=${MYSQL_USER}" >> /root/hue.ini
#echo "password=${MYSQL_PASSWORD}" >> /root/hue.ini
#echo "name=mysql" >> /root/hue.ini

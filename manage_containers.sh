#!/bin/bash

# Detener contenedores específicos
docker stop interfacev2_rqworker_1 interfacev2_web_1 interfacev2_redis_1

# Eliminar contenedores específicos
docker rm interfacev2_rqworker_1 interfacev2_web_1 interfacev2_redis_1

# Limpiar el sistema Docker sin confirmación
docker system prune -f

# Navegar al directorio del proyecto
cd /var/www/interfacev2

# Activar el entorno virtual de Python
source venv/bin/activate

# Levantar los servicios con docker-compose
docker-compose -f docker-compose.rq.yml up -d --build

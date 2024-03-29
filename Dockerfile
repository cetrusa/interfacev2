# Usamos la imagen oficial de Python 3.12 como imagen base
FROM python:3.12

# Crea un usuario y un grupo para ejecutar la aplicación
RUN groupadd -r interfacegroup && useradd -r -g interfacegroup adminuser

# Establecemos /code como el directorio de trabajo dentro del contenedor
WORKDIR /code

# Configuramos variables de entorno
ENV PIP_DISABLE_PIP_VERSION_CHECK 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Copiamos requirements.txt a /code en el contenedor
COPY ./requirements.txt .

# Instalamos las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todos los archivos y directorios al directorio de trabajo en el contenedor
COPY . .

# Ajustamos la zona horaria del contenedor
RUN ln -sf /usr/share/zoneinfo/America/Bogota /etc/localtime

# Ejecutamos el comando collectstatic de Django
RUN python manage.py collectstatic --no-input

# Cambiamos la propiedad y los permisos de los archivos estáticos
# Asegúrate de que la ruta es correcta para tus archivos estáticos
RUN chown -R adminuser:interfacegroup /code/static
RUN chmod -R 755 /code/static

# Crea la carpeta media y establece permisos adecuados
RUN mkdir -p /code/media
RUN chown -R adminuser:interfacegroup /code/media
RUN chmod -R 755 /code/media

# Cambiar al usuario no root
USER adminuser

# Aquí irían las instrucciones para ejecutar tu aplicación Django

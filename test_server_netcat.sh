#!/bin/bash

# Script para verificar el funcionamiento del servidor usando netcat dentro de un contenedor
# sin exponer puertos al equipo anfitrión ni editar docker-compose.yaml

# Obtener el nombre de la red docker creada por docker-compose
NETWORK_NAME=$(docker network ls --filter name=tp-nivelador --format "{{.Name}}" | head -n 1)

if [ -z "$NETWORK_NAME" ]; then
    echo "Error: No se encontró la red docker del proyecto. Asegúrate de ejecutar 'make up' primero."
    exit 1
fi

echo "Usando red docker: $NETWORK_NAME"
echo "Enviando 'Hello World' al servidor usando netcat desde un contenedor..."

# Ejecutar un contenedor temporal con netcat, conectado a la red del proyecto
# y enviar el mensaje al servidor (con timeout de 2 segundos)
docker run --rm --network "$NETWORK_NAME" alpine:latest sh -c "echo 'Hello World' | nc -w 2 server 5678"

echo ""
echo "Prueba completada."

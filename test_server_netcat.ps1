# Script para verificar el funcionamiento del servidor usando netcat dentro de un contenedor
# sin exponer puertos al equipo anfitrión ni editar docker-compose.yaml

# Obtener el nombre de la red docker creada por docker-compose
$networkName = docker network ls --filter name=tp-nivelador --format "{{.Name}}" | Select-Object -First 1

if ([string]::IsNullOrEmpty($networkName)) {
    Write-Error "Error: No se encontró la red docker del proyecto. Asegúrate de ejecutar 'make up' primero."
    exit 1
}

Write-Host "Usando red docker: $networkName"
Write-Host "Enviando 'Hello World' al servidor usando netcat desde un contenedor..."

# Ejecutar un contenedor temporal con netcat, conectado a la red del proyecto
# y enviar el mensaje al servidor (con timeout de 2 segundos)
docker run --rm --network $networkName alpine:latest sh -c "echo 'Hello World' | nc -w 2 server 5678"

Write-Host ""
Write-Host "Prueba completada."

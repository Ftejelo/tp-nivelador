## Protocolo

El intercambio de mensajes usa un formato binario con header fijo y payload variable:

- 1 byte: tipo de mensaje
- 4 bytes: longitud del payload en big-endian
- payload: contenido del mensaje

Los tipos de mensaje son:

- BET (1): envío de una o más apuestas en batch
- FINISH (2): notifica que una agencia terminó de enviar sus apuestas
- WINNERS (3): devuelve el conjunto de ganadores para esa agencia

La serialización de cada apuesta incluye:

- agency_id: int32
- first_name, last_name, birthdate: strings como longitud (uint16) + bytes
- document, number: int32

En el caso de BET y WINNERS, el payload empieza con un contador de apuestas seguido por la secuencia de registros.\
Esto permite que el servidor reconstruya el batch completo y lo procese como una unidad. La deserialización valida longitudes y truncamientos para detectar errores de lectura y evitar inconsistencias.

Tanto el cliente como el servidor usan rutinas de envío y recepción que loopean hasta completar el tamaño requerido. Esto evita los escenarios de short read y short write.

## Sincronización de la ejecución concurrente

El servidor está diseñado para atender múltiples agencias concurrentemente:

- un (`accept loop`) queda escuchando nuevas conexiones
- por cada cliente se crea un hilo de trabajo que atiende su flujo de mensajes

La sincronización del estado compartido se realiza con `threading.Lock` y `threading.Condition`:

- `agencies_finished` cuenta cuántas agencias ya enviaron su FINISH
- `agencies_finished_lock` protege el acceso a ese contador
- `quorum_condition` bloquea los hilos que esperan a que se alcance el quorum configurado por `AGENCY_QUORUM_MIN`

Cuando un cliente llega al FINISH, el servidor:

1. incrementa el contador de agencias terminadas
2. espera hasta alcanzar el quorum mínimo
3. calcula los ganadores una sola vez
4. notifica a los demás hilos con `notify_all()` para que continúen y reciban el mismo resultado

## Cálculo de ganadores y respuesta

Cuando se alcanza el quorum, el servidor recorre todas las apuestas almacenadas y aplica la lógica de la lotería (`has_won`).\
Luego, filtra los ganadores por agencia y devuelve únicamente la lista correspondiente a cada cliente. Así, no se hace broadcast de resultados.

## Cierre graceful

- el servidor registra handlers para `SIGTERM` e `SIGINT`
- se activa un flag de apagado (`shutdown_requested`) y un `Event` para avisar a los hilos
- el cliente en Go usa `context.Context` y `signal.Notify`.

La combinación entre condiciones de espera, cierre de sockets y espera acotada de hilos permite que el sistema finalice sin dejar recursos abiertos ni interrumpir arbitrariamente la comunicación.

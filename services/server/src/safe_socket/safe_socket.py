import socket


def recv_all(socket: socket.socket, size):
    data = bytearray()
    while len(data) < size:
        chunk = socket.recv(size - len(data))
        if not chunk:
            raise ConnectionError("Connection closed before receiving all data")
        data.extend(chunk)
    return bytes(data)


def send_all(socket: socket.socket, bytes):
    total_sent = 0
    while total_sent < len(bytes):
        sent = socket.send(bytes[total_sent:])
        if sent == 0:
            raise ConnectionError("Connection closed before sending all data")
        total_sent += sent
    return total_sent

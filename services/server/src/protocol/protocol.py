import struct
from dataclasses import dataclass
from typing import List

import safe_socket


@dataclass
class Bet:
    agency_id: int
    first_name: str
    last_name: str
    document: int
    birthdate: str
    number: int


@dataclass
class BetMessage:
    bets: List[Bet]


@dataclass
class FinishMessage:
    pass


@dataclass
class WinnersMessage:
    winners: List[Bet]


class MessageType:
    BET = 1
    FINISH = 2
    WINNERS = 3


def _encode_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack(">H", len(encoded)) + encoded


def _decode_string(payload: bytes, offset: int):
    if offset + 2 > len(payload):
        raise ValueError("truncated string length")
    length = struct.unpack(">H", payload[offset : offset + 2])[0]
    offset += 2
    if offset + length > len(payload):
        raise ValueError("truncated string payload")
    value = payload[offset : offset + length].decode("utf-8")
    return value, offset + length


def serialize_bet(bet: Bet) -> bytes:
    payload = bytearray()
    payload.extend(struct.pack(">i", bet.agency_id))
    payload.extend(_encode_string(bet.first_name))
    payload.extend(_encode_string(bet.last_name))
    payload.extend(struct.pack(">i", bet.document))
    payload.extend(_encode_string(bet.birthdate))
    payload.extend(struct.pack(">i", bet.number))
    return bytes(payload)


def deserialize_bet(payload: bytes, offset: int = 0):
    if offset + 4 > len(payload):
        raise ValueError("truncated bet header")
    agency_id = struct.unpack(">i", payload[offset : offset + 4])[0]
    offset += 4

    first_name, offset = _decode_string(payload, offset)
    last_name, offset = _decode_string(payload, offset)

    if offset + 4 > len(payload):
        raise ValueError("truncated bet document")
    document = struct.unpack(">i", payload[offset : offset + 4])[0]
    offset += 4

    birthdate, offset = _decode_string(payload, offset)

    if offset + 4 > len(payload):
        raise ValueError("truncated bet number")
    number = struct.unpack(">i", payload[offset : offset + 4])[0]
    offset += 4

    return Bet(
        agency_id=agency_id,
        first_name=first_name,
        last_name=last_name,
        document=document,
        birthdate=birthdate,
        number=number,
    ), offset


def serialize_bets(bets: List[Bet]) -> bytes:
    payload = bytearray()
    payload.extend(struct.pack(">I", len(bets)))
    for bet in bets:
        payload.extend(serialize_bet(bet))
    return bytes(payload)


def deserialize_bets(payload: bytes, offset: int = 0):
    if offset + 4 > len(payload):
        raise ValueError("truncated bets count")
    bet_count = struct.unpack(">I", payload[offset : offset + 4])[0]
    offset += 4

    bets = []
    for _ in range(bet_count):
        bet, offset = deserialize_bet(payload, offset)
        bets.append(bet)
    return bets, offset


def serialize_message(msg_type: int, payload) -> bytes:
    if msg_type == MessageType.BET:
        payload_bytes = serialize_bets(payload.bets)
    elif msg_type == MessageType.WINNERS:
        payload_bytes = serialize_bets(payload.winners)
    elif msg_type == MessageType.FINISH:
        payload_bytes = b""
    else:
        raise ValueError(f"Unknown message type: {msg_type}")

    return struct.pack(">B", msg_type) + struct.pack(">I", len(payload_bytes)) + payload_bytes


def recv_message(socket) -> tuple:
    header = safe_socket.recv_all(socket, 5)
    msg_type = header[0]
    payload_length = struct.unpack(">I", header[1:5])[0]
    payload_bytes = safe_socket.recv_all(socket, payload_length)

    if msg_type == MessageType.BET:
        bets, _ = deserialize_bets(payload_bytes)
        return msg_type, BetMessage(bets=bets)
    if msg_type == MessageType.FINISH:
        return msg_type, FinishMessage()
    if msg_type == MessageType.WINNERS:
        winners, _ = deserialize_bets(payload_bytes)
        return msg_type, WinnersMessage(winners=winners)

    raise ValueError(f"Unknown message type: {msg_type}")


def send_message(socket, msg_type: int, payload) -> None:
    message_bytes = serialize_message(msg_type, payload)
    safe_socket.send_all(socket, message_bytes)

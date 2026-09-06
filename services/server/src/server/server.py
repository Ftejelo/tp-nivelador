import os
import socket

import logger
import protocol
import safe_socket
from lottery.bet import Bet
from lottery.lottery import Lottery


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.storage_path = os.environ.get("LOTTERY_STORAGE_PATH", "/tmp/lottery_bets.csv")
        self.lottery = Lottery(self.storage_path)

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets_received = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                msg_type, payload = protocol.recv_message(client_socket)
                
                if msg_type == protocol.MessageType.BET:
                    # Convert protocol bets to lottery bets
                    lottery_bets = []
                    for bet in payload.bets:
                        lottery_bets.append(Bet(
                            agency_id=bet.agency_id,
                            first_name=bet.first_name,
                            last_name=bet.last_name,
                            document=bet.document,
                            birthdate=bet.birthdate,
                            number=bet.number,
                        ))
                    
                    # Store bets using Lottery class
                    self.lottery.store_bets(lottery_bets)
                    bets_received += len(lottery_bets)
                    
                elif msg_type == protocol.MessageType.FINISH:
                    # Calculate winners
                    winners = []
                    for bet in self.lottery.load_bets():
                        if self.lottery.has_won(bet):
                            winners.append(bet)
                    
                    # Convert lottery bets to protocol bets
                    protocol_winners = []
                    for winner in winners:
                        protocol_winners.append(protocol.Bet(
                            agency_id=winner.agency_id,
                            first_name=winner.first_name,
                            last_name=winner.last_name,
                            document=winner.document,
                            birthdate=winner.birthdate,
                            number=winner.number,
                        ))
                    
                    # Send winners back to client
                    winners_msg = protocol.WinnersMessage(winners=protocol_winners)
                    protocol.send_message(client_socket, protocol.MessageType.WINNERS, winners_msg)
                    
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "bets-received",
                        bets_received,
                        "winners-sent",
                        len(winners),
                    )
                    return
                    
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "bets-received", bets_received
            )
            raise e

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)

import os
import socket
import threading

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
        self.quorum_min = int(os.environ.get("AGENCY_QUORUM_MIN", "1"))
        
        # Thread synchronization
        self.agencies_finished = 0
        self.agencies_finished_lock = threading.Lock()
        self.quorum_condition = threading.Condition(self.agencies_finished_lock)
        self.winners_calculated = False
        self.winners = []

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets_received = 0
        client_agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                msg_type, payload = protocol.recv_message(client_socket)
                
                if msg_type == protocol.MessageType.BET:
                    # Convert protocol bets to lottery bets
                    lottery_bets = []
                    for bet in payload.bets:
                        if client_agency_id is None:
                            client_agency_id = bet.agency_id
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
                    # Increment finished agencies counter
                    with self.agencies_finished_lock:
                        self.agencies_finished += 1
                        logger.info(
                            "agency-finished",
                            logger.LogResult.success,
                            "agency-id",
                            client_agency_id,
                            "agencies-finished",
                            self.agencies_finished,
                            "quorum-min",
                            self.quorum_min,
                        )
                        
                        # Wait for quorum to be reached
                        while self.agencies_finished < self.quorum_min:
                            self.quorum_condition.wait()
                        
                        # Calculate winners only once
                        if not self.winners_calculated:
                            self.winners = []
                            for bet in self.lottery.load_bets():
                                if self.lottery.has_won(bet):
                                    self.winners.append(bet)
                            self.winners_calculated = True
                            logger.info(
                                "lottery-draw",
                                logger.LogResult.success,
                                "total-winners",
                                len(self.winners),
                            )
                        
                        # Notify other waiting threads
                        self.quorum_condition.notify_all()
                    
                    # Filter winners for this agency only
                    agency_winners = [
                        winner for winner in self.winners 
                        if winner.agency_id == client_agency_id
                    ]
                    
                    # Convert lottery bets to protocol bets
                    protocol_winners = []
                    for winner in agency_winners:
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
                        len(agency_winners),
                        "agency-id",
                        client_agency_id,
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

                # Handle client in a separate thread
                client_thread = threading.Thread(target=self._handle_client, args=(client_socket,))
                client_thread.start()

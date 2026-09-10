import os
import signal
import socket
import threading
import time

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
        
        # Graceful shutdown
        self.shutdown_requested = False
        self.shutdown_event = threading.Event()
        self.client_threads = []
        self.client_sockets = []
        self.client_threads_lock = threading.Lock()

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets_received = 0
        client_agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                # Check for shutdown before blocking on recv
                if self.shutdown_requested:
                    logger.info(action, logger.LogResult.fail, "reason", "shutdown requested")
                    return
                
                try:
                    msg_type, payload = protocol.recv_message(client_socket)
                except socket.timeout:
                    # Timeout is expected, loop to check shutdown flag
                    continue
                
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
                            if self.shutdown_requested:
                                logger.info(
                                    "agency-finished",
                                    logger.LogResult.fail,
                                    "agency-id",
                                    client_agency_id,
                                    "reason",
                                    "shutdown requested before quorum",
                                )
                                return
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
        finally:
            client_socket.close()
            # Remove thread and socket from tracking lists
            with self.client_threads_lock:
                if threading.current_thread() in self.client_threads:
                    self.client_threads.remove(threading.current_thread())
                if client_socket in self.client_sockets:
                    self.client_sockets.remove(client_socket)

    def _signal_handler(self, signum, frame):
        action = "shutdown"
        logger.info(action, logger.LogResult.in_progress, "signal", signum)
        self.shutdown_requested = True
        self.shutdown_event.set()
        with self.agencies_finished_lock:
            self.quorum_condition.notify_all()

    def _wait_for_client_threads(self, timeout=10):
        action = "wait-client-threads"
        logger.info(action, logger.LogResult.in_progress, "timeout", timeout)
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self.client_threads_lock:
                if not self.client_threads:
                    logger.info(action, logger.LogResult.success, "threads-remaining", 0)
                    return True
            time.sleep(0.1)
        
        with self.client_threads_lock:
            remaining = len(self.client_threads)
            logger.warn(action, logger.LogResult.fail, "threads-remaining", remaining)
        return False

    def _accept_loop(self, server_socket):
        action = "accept-connection"
        while not self.shutdown_requested:
            try:
                logger.info(action, logger.LogResult.in_progress)
                client_socket, _ = server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                if self.shutdown_requested:
                    break
                raise
            except Exception as e:
                if self.shutdown_requested:
                    break
                logger.error(action, logger.LogResult.fail)
                raise e

            logger.info(action, logger.LogResult.success)
            client_socket.settimeout(1.0)
            client_thread = threading.Thread(
                target=self._handle_client,
                args=(client_socket,),
                daemon=True,
            )
            client_thread.start()

            with self.client_threads_lock:
                self.client_threads.append(client_thread)
                self.client_sockets.append(client_socket)

    def run(self):
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            server_socket.settimeout(1.0)

            logger.info(
                "server-start",
                logger.LogResult.success,
                "host",
                self.server_host,
                "port",
                self.server_port,
            )

            accept_thread = threading.Thread(
                target=self._accept_loop,
                args=(server_socket,),
                daemon=True,
            )
            accept_thread.start()

            while not self.shutdown_requested:
                self.shutdown_event.wait(0.2)

            logger.info("server-shutdown", logger.LogResult.in_progress)
            server_socket.close()

            with self.client_threads_lock:
                for sock in list(self.client_sockets):
                    try:
                        sock.close()
                    except Exception:
                        pass

            self._wait_for_client_threads(timeout=5)
            accept_thread.join(timeout=5)
            logger.info("server-shutdown", logger.LogResult.success)

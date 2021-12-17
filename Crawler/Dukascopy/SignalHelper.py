import os
import signal
import sys


class SignalAndLockHandler:
    terminate_signal_received = False
    intercept_termination = False

    def __init__(self, lock_file=None):

        if lock_file is not None:
            if os.path.exists(lock_file):
                print("Lockdatei gefunden!")
                if input("Dennoch fortfahren? (y|N) ") != "y":
                    print("Abbruch.")
                    sys.exit(1)
            with open(lock_file, "w") as f:
                pass

        self.lock_file = lock_file
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):

        # Lockdatei entfernen
        if self.lock_file is not None and os.path.exists(self.lock_file):
            os.unlink(self.lock_file)

        # Signal nicht unterbinden
        if not self.intercept_termination:
            print("\n** Abbruch.")
            sys.exit(0)

        if self.terminate_signal_received:
            print("\n!!! Erneutes SIGTERM/SIGINT empfangen. Sofortiger Abbruch.")
            sys.exit(1)

        print("\n** SIGTERM/SIGINT empfangen. Abbruch nach aktueller Aufgabe.")
        self.terminate_signal_received = True


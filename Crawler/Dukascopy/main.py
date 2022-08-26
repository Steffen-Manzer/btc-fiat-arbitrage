#!/usr/bin/env python3

# Geschrieben für Python 3.9 unter macOS 12.1
# Mit Hilfe von https://github.com/terukusu/download-tick-from-dukascopy

from datetime import datetime, timedelta, timezone
import os.path
import lzma
import re
import SignalHelper
import struct
import subprocess
import sys
from TerminalColors import TerminalColors
from urllib import request

# ====== Beginn Konfiguration ======

# Ausgabepfad (Einzelne Paare werden in Unterverzeichnisse abgelegt)
out_path = "../../Daten/dukascopy-tick/"

# Gewünschte Wechselpaare
symbols = ["eurusd"]  # , "gbpusd", "usdchf", "usdcad", "usdjpy", "audusd"]

# Alle Daten ab 01.01.2010 bis letzten Monat abfragen
now = datetime.now()
start_date = datetime(year=2010, month=1, day=1, tzinfo=timezone.utc)
if now.month == 1:
    end_date = datetime(year=now.year - 1, month=12, day=1, tzinfo=timezone.utc)
else:
    end_date = datetime(year=now.year, month=now.month - 1, day=1, tzinfo=timezone.utc)


# ====== Ende Konfiguration ======


# LZMA entpacken
def decompress_lzma(data):
    results = []
    decomp = lzma.LZMADecompressor()
    while True:
        try:
            res = decomp.decompress(data)
        except lzma.LZMAError:
            if results:
                break
            else:
                raise
        results.append(res)
        data = decomp.unused_data
        if not data:
            break
        if not decomp.eof:
            raise lzma.LZMAError("Compressed data ended before the end-of-stream marker was reached")
    return b"".join(results)


# 20 Byte-Segmente entpacken
def process_fragments(buffer):
    token_size = 20
    token_count = int(len(buffer) / token_size)
    tokens = list(map(lambda x: struct.unpack_from('>3I2f', buffer, token_size * x), range(0, token_count)))
    return tokens


# Tickdaten korrigieren (Bid/Ask und Volumen korrekt berechnen)
def normalize_tick(symbol: str, day: datetime, time, ask, bid, ask_vol, bid_vol):
    date = day + timedelta(milliseconds=time)

    # 1 Pip ist für bestimmte Währungen nur 1/1000
    if any(map(lambda x: x in symbol.lower(), ['usdrub', 'jpy'])):
        point = 1000
    else:
        point = 100000

    return {
        "date": date,
        "ask": ask / point,
        "bid": bid / point,
        "ask_volume": round(ask_vol * 1000000),
        "bid_volume": round(bid_vol * 1000000)
    }


# Tickdaten herunterladen und verarbeiten
def download_ticks(symbol: str, day: datetime):
    # Siehe:
    # https://github.com/ninety47/dukascopy und
    # https://stackoverflow.com/questions/41176164/decompress-and-read-dukascopy-bi5-tick-files
    url_prefix = 'http://datafeed.dukascopy.com/datafeed'

    ticks_day = []
    for hour in range(0, 24):
        file_name = f'{hour:02d}h_ticks.bi5'
        url = f'{url_prefix}/{symbol.upper()}/{day.year:04d}/{day.month - 1:02d}/{day.day:02d}/{file_name}'

        req = request.Request(url)
        with request.urlopen(req) as res:
            res_body = res.read()

        if len(res_body):
            uncompressed_data = decompress_lzma(res_body)
        else:
            uncompressed_data = []

        tokenized_data = process_fragments(uncompressed_data)
        ticks_hour = list(map(lambda x: normalize_tick(symbol, day + timedelta(hours=hour), *x), tokenized_data))
        ticks_day.extend(ticks_hour)

    return ticks_day


# Tickdaten als CSV ausgeben
def ticks_to_csv(ticks: list):
    # Keine Leerzeilen einfügen, wenn Datensatz leer ist
    if len(ticks) == 0:
        return ""

    return "\n".join(
        map(
            lambda x: ",".join([
                x['date'].isoformat().replace('+00:00', 'Z'),
                str(x['bid']),
                str(x['ask']),
                str(x['bid_volume']),
                str(x['ask_volume'])
            ]),
            ticks
        )
    ) + "\n"


# Aktuellen Monat überspringen, nächsten Monat suchen
def skip_to_next_month(date: datetime):
    current_month = date.month
    while date.month == current_month:
        date += timedelta(days=1)
    return date


# Starte Abfrage
if __name__ == '__main__':

    # CTRL+C / Interrupts abfragen und unvollständige Datensätze löschen
    signalHandler = SignalHelper.SignalAndLockHandler()
    signalHandler.intercept_termination = True

    for symbol in symbols:
        if signalHandler.terminate_signal_received:
            print("\nAbbruch.")
            sys.exit(1)

        print(f"{TerminalColors.BOLD}Verarbeite {symbol.upper()}{TerminalColors.NORMAL}")

        output_dir = os.path.realpath(f"{out_path}{symbol}/")
        current_date = start_date

        if os.path.exists(output_dir):

            # Ausgabeverzeichnis existiert, ggf. sind bereits Daten vorhanden.
            # Letzten vollständigen Datensatz finden und ab dort beginnen.
            files = os.listdir(output_dir)
            files.sort(reverse=True)
            search_regex = re.compile(r"dukascopy-\w{6}-(\d{4})-(\d{2})\.csv\.gz")
            for file in files:
                match = search_regex.match(file)
                if match is not None:
                    # Starte nach letztem Datensatz
                    current_date = datetime(year=int(match[1]), month=int(match[2]), day=1, tzinfo=timezone.utc)
                    print(f"Daten verfügbar bis einschließlich {current_date.strftime('%Y-%m')}")
                    current_date = skip_to_next_month(current_date)
                    break

        else:
            # Ausgabeverzeichnis existiert nicht, anlegen.
            # Keine Daten vorhanden, starte bei start_date.
            os.mkdir(output_dir)

        new_data_found = False
        while current_date <= end_date:

            if signalHandler.terminate_signal_received:
                print("\nAbbruch.")
                sys.exit(1)

            # CSV-Pfad bestimmen
            output_csv = f'{output_dir}/dukascopy-{symbol}-{current_date.strftime("%Y-%m")}.csv'

            # Dieser Datensatz existiert bereits, überspringe.
            if os.path.exists(output_csv) or os.path.exists(f"{output_csv}.gz"):
                # print(f"{current_date.strftime('%Y-%m')} existiert bereits, überspringe.")
                current_date = skip_to_next_month(current_date)
                continue

            # Daten laden, verarbeiten und schreiben
            print(f"Lade Daten für {current_date.strftime('%Y-%m')}...")
            new_data_found = True
            current_month = current_date.month
            with open(output_csv, 'w') as f:
                f.write('Time,Bid,Ask,BidVolume,AskVolume\n')
                while current_date.month == current_month:
                    ticks = download_ticks(symbol, current_date)
                    f.write(ticks_to_csv(ticks))
                    current_date += timedelta(days=1)

                    if signalHandler.terminate_signal_received:
                        f.close()
                        os.unlink(output_csv)
                        print("\nProzess beendet und Ausgabedatei gelöscht.")
                        sys.exit(1)

            # CSV komprimieren
            subprocess.run(['gzip', output_csv])

        if not new_data_found:
            print(f"{TerminalColors.YELLOW}Keine neuen Daten vorhanden.{TerminalColors.NORMAL}")

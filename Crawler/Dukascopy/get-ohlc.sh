#!/bin/sh

# Konfiguration
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TARGET_PATH=$(readlink -f "${SCRIPT_DIR}/../../Daten/dukascopy-ohlc")
LOAD_UNTIL=$(date +%Y-%m-%d)

echo "Download bis ${LOAD_UNTIL} nach ${TARGET_PATH}."

npx dukascopy-node -i eurusd -from 1998-12-01 -to "${LOAD_UNTIL}" -t mn1 -f csv -dir "${TARGET_PATH}"
npx dukascopy-node -i usdjpy -from 1989-12-01 -to "${LOAD_UNTIL}" -t mn1 -f csv -dir "${TARGET_PATH}"
npx dukascopy-node -i gbpusd -from 1989-12-01 -to "${LOAD_UNTIL}" -t mn1 -f csv -dir "${TARGET_PATH}"
npx dukascopy-node -i usdchf -from 1989-12-01 -to "${LOAD_UNTIL}" -t mn1 -f csv -dir "${TARGET_PATH}"

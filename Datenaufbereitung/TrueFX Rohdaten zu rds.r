# Lese und aggregiere TrueFX-Rohdaten zu
# - Tickdaten (vollständig)
# - 1s-/5s-/60s/1d: Schlusskurse für Bid-, Ask- und Mittelkurs
# Diese Auswahl erfolgt, um die Dateigröße zu gering wie möglich zu halten.

source("Funktionen/ReadMonthlyDividedDataset.r")

# Bibliotheken laden
library("data.table") # fread
library("lubridate") # parse_date_time
library("dplyr") # summarise

# Hilfsfunktion: Eine .csv-Datei einlesen
parseTrueFXTickData <- function(srcFile) {
    
    # Tickdaten aus CSV einlesen
    # Datenstruktur:
    #       Paar                  Time     Bid     Ask
    # 1: EUR/USD 20211101 19:07:40.498 1.16034 1.16037
    # 2: EUR/USD 20211101 19:07:42.231 1.16033 1.16037
    thisDataset <- fread(
        cmd=paste0("unzip -cq ", srcFile),
        showProgress = FALSE,
        header = FALSE,
        col.names = c("Paar", "Time", "Bid", "Ask")
    )
    
    # Entferne erste Spalte, enthält nur Namen des Datensatzes
    thisDataset$Paar <- NULL
    
    # Lese Datum
    # Originalformat: 20180101 22:01:01.051
    # Variante 1: parse_date_time(data$Time, "%Y%m%d %H:%M:%OS") (ca. 2,5-3s)
    # Variante 2: Bindestriche einfügen und fastPOSIXct: Langsamer
    thisDataset$Time <- parse_date_time(thisDataset$Time, "%Y%m%d %H:%M:%OS")
    
    # Mittelkurs aus Bid und Ask berechnen
    thisDataset$Mittel <- rowMeans(thisDataset[,c("Bid","Ask")])
    
    return(thisDataset)
    
}

# Hilfsfunktion: Daten eines Zeitabschnittes aggregieren
summariseTrueFXTickData <- function(thisDataset) {
    thisDataset |> summarise(
        CloseBid = last(Bid),
        CloseAsk = last(Ask),
        CloseMittel = last(Mittel),
        numDatasets = n(),
    )
}

# Daten einlesen und aggregieren
readMonthlyDividedDataset(
    "TrueFX",
    currencyPairs = c("EURUSD", "GBPUSD", "USDJPY"),
    getSourceFileCallback = function(pair, year, month) {
        return(paste0(
            "Daten/",
            "truefx/",
            pair, "/",
            pair, "-", year, "-", sprintf("%02d", month), ".zip"
        ))
    },
    parseSourceFileCallback = parseTrueFXTickData,
    summariseDataCallback = summariseTrueFXTickData
)


source("Datenaufbereitung/0 ReadMonthlyDividedDataset.r")

parseDukascopyTickData <- function(srcFile) {
    
    # Bibliotheken laden
    library("data.table")
    
    # Tickdaten aus CSV einlesen
    # Datenstruktur:
    #        timestamp askPrice bidPrice askVolume bidVolume
    # 1: 1627851854474  1.18722  1.18641      0.37      0.19
    # 2: 1627851862067  1.18722  1.18641      0.37      0.19
    thisDataset <- fread(srcFile, showProgress = FALSE)
    
    # Handelsvolumen interessiert derzeit nicht
    thisDataset$bidVolume <- NULL
    thisDataset$askVolume <- NULL
    
    # Spaltenbezeichnungen normalisieren
    colnames(thisDataset)[1] <- "Time"
    colnames(thisDataset)[2] <- "Ask"
    colnames(thisDataset)[3] <- "Bid"
    
    # Zeit einlesen
    # as.numeric und +0.1, um floating point-Probleme zu vermeiden
    # Siehe Beispiele in der Dokumentation von as.POSIXct:
    # # avoid rounding down: milliseconds are not exactly representable
    # as.POSIXct((z+0.1)/1000, origin = "1960-01-01")
    thisDataset$Time <- as.POSIXct((as.numeric(thisDataset$Time)+0.1)/1000, origin = "1970-01-01")
    
    # Mittelkurs aus Bid und Ask berechnen
    thisDataset$Mittel <- rowMeans(thisDataset[,c("Ask","Bid")])
    
    return(thisDataset)
    
}

# Lese und aggregiere Dukascopy-Rohdaten zu
# - Tickdaten (vollständig)
# - 1s-/5s-/60s/1d: Schlusskurse für Bid-, Ask- und Mittelkurs
# Diese Auswahl erfolgt, um die Dateigröße zu gering wie möglich zu halten.
summariseDukascopyTickData <- function(dataset) {
    dataset |> summarise(
        CloseBid = last(Bid),
        CloseAsk = last(Ask),
        CloseMittel = last(Mittel),
        numDatasets = n(),
    )
}

readMonthlyDividedDataset(
    "Dukascopy",
    currencyPairs = c("AUDUSD", "EURUSD", "GBPUSD", "USDCAD", "USDCHF", "USDJPY"),
    getSourceFileCallback = function(pair, year, month) {
        return(paste0(
            "Daten/",
            "dukascopy/",
            pair, "/",
            pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFileCallback = parseDukascopyTickData,
    summariseDataCallback = summariseDukascopyTickData
)

cat("\nTipp: R-Session neu starten, um nicht benötigten Speicher freizugeben.\n")

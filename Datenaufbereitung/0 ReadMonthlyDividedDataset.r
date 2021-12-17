#' Einen Datensatz einlesen, der in monatlich getrennten CSV-Dateien gespeichert ist.
#' Funktioniert für Bitcoin-Börsen genau wie für Wechselkurse.
#' 
#' @author Steffen Manzer
#' @param exchangeName Name der Börse.
#' @param currencyPairs Liste der Gegenwährungen, z.B. c("btceur", "eurusd")
#' @param getSourceFileCallback Callback, das den Dateipfad der CSV-Quelldatei erzeugt
#' @param parseSourceFileCallback Callback, das die CSV-Quelldatei einliest
#' @param summariseDataCallback Callback, das die Daten nach 1s/5s/... aggregiert
#' @param targetBasename Basisverzeichnis für Zieldateien. Standard: exchangeName in Kleinbuchstaben, falls NA.
#' @param sourceFileExtension Dateiendung der Rohdaten. Standard: .csv.gz.
#' @param readUntil Daten bis zu diesem Zeitpunkt einlesen. Standard: bis vor einen Monat.
#' @return NULL
#' @examples
#' readMonthlyDividedDataset(
#'     exchangeName = "Bitstamp",
#'     currencyPairs = c("btcusd", "btceur"),
#'     getSourceFileCallback = function(pair, date) {
#'         return(paste0(
#'             "Daten/"bitstamp/", pair, "/",
#'             "bitfinex-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
#'         ))
#'     },
#'     parseSourceFileCallback = function(srcFile) {
#'          #                            ID         Time         Amount    Price
#'          sourceFileColumnClasses = c("numeric", "character", "double", "double")
#'          thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
#'          thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
#'         return(thisDataset)
#'     },
#'     readUntil = as.POSIXct("2019-08-01")
#' )
readMonthlyDividedDataset <- function(
    exchangeName,
    currencyPairs,
    getSourceFileCallback,
    parseSourceFileCallback,
    summariseDataCallback,
    targetBasename = NA,
    sourceFileExtension = ".csv.gz",
    readUntil = NA
) {
    
    # Bibliotheken laden ------------------------------------------------------
    library("data.table")
    library("dplyr")
    library("fasttime")
    library("tictoc")
    library("lubridate") # floor_date
    
    # Parameter verarbeiten ---------------------------------------------------
    if (is.na(targetBasename)) {
        targetBasename = exchangeName |> tolower()
    }
    
    if (!is.na(readUntil)) {
        
        # Daten nur bis zu einem bestimmten Datum einlesen (einschließlich)
        endMonth = month(readUntil)
        endYear = year(readUntil)
        
    } else {
        
        # Daten bis vor einem Monat einlesen, da aktueller Monat nicht
        # zwingend vollständig vorliegt. Das wäre deshalb problematisch,
        # weil eine Aggregation auf Monatsebene vorgenommen wird.
        endMonth <- month(Sys.time())
        endYear <- year(Sys.time())
        if (endMonth == 1) {
            endYear <- endYear - 1
            endMonth <- 12
        } else {
            endMonth <- endMonth - 1
        }
        
    }
    
    # Daten einlesen ----------------------------------------------------------
    # Jedes Wechselkurspaar dieser Börse durchgehen
    for (pair in currencyPairs) {
        
        newDataFound <- FALSE
        cat("===== ", exchangeName, " ", toupper(pair), " =====\n", sep="")
        
        # Cache-Basisverzeichnis
        cacheBase <- paste0("Cache/", targetBasename, "/", tolower(pair), "/")
        
        # Cache-Verzeichnisse anlegen
        for (aggregationLevel in c("tick", "1s", "5s", "60s")) {
            if (!dir.exists(paste0(cacheBase, aggregationLevel))) {
                dir.create(paste0(cacheBase, aggregationLevel), recursive = TRUE)
            }
        }
        
        # Basispfade für alle Aggregationsstufen festlegen
        # Genauere Aggregationsstufen werden nach Monaten getrennt
        # Beispiel-Schema: Cache/bitstamp/btcusd/tick/bitstamp-btcusd-tick-2019-09.rds
        cacheBaseTick <- paste0(cacheBase, "tick/", targetBasename, "-", tolower(pair), "-tick-")
        cacheBase1s   <- paste0(cacheBase, "1s/",   targetBasename, "-", tolower(pair), "-1s-")
        cacheBase5s   <- paste0(cacheBase, "5s/",   targetBasename, "-", tolower(pair), "-5s-")
        cacheBase60s  <- paste0(cacheBase, "60s/",  targetBasename, "-", tolower(pair), "-60s-")
        
        # Tages- und Monatsdaten werden nicht nach Monaten getrennt,
        # sondern alle in eine Datei geschrieben
        # Beispiel-Schema: Cache/bitstamp/btcusd/bitstamp-btcusd-daily.rds
        targetFileDaily <- paste0(cacheBase, targetBasename, "-", tolower(pair), "-daily.rds")
        targetFileMonthly <- paste0(cacheBase, targetBasename, "-", tolower(pair), "-monthly.rds")
        
        
        # Anhand der (sehr kleinen) Tagesdaten prüfen, ob der Datensatz 
        # bereits einmal eingelesen wurde und nur neue Daten
        # angehängt werden müssen.
        if (file.exists(targetFileDaily)) {
            
            if (!file.exists(targetFileMonthly)) {
                cat("Monatsdaten fehlen, obwohl Tagesdaten verfügbar sind. Fehler!")
                next
            }
            
            dataset_daily <- readRDS(targetFileDaily)
            dataset_monthly <- readRDS(targetFileMonthly)
            
            lastDataset = last(dataset_daily$Time)
            lastMonth = month(lastDataset)
            lastYear = year(lastDataset)
            
            cat("Datensatz vorhanden, letzter Stand:", lastMonth, "/", lastYear, "...")
            
            # Größer-gleich-Vergleich, da Enddatum vor dem Ende 
            # der bereits eingelesenen Daten liegen kann
            if (lastYear >= endYear && lastMonth >= endMonth) {
                cat(" Überspringe.\n")
                next
            }
            
            if (lastMonth == 12) {
                startYear <- lastYear + 1
                startMonth <- 1
            } else {
                startYear <- lastYear
                startMonth <- lastMonth + 1
            }
            
            cat("\n")
            
        } else {
            
            # Datensatz nicht gefunden, starte von Beginn.
            # Ab 2011 starten, manche Datensätze beginnen so früh.
            dataset_daily <- data.table()
            dataset_monthly <- data.table()
            startYear = 2010
            startMonth = 1
            
        }
        
        # Jedes Jahr der (möglichen) Quelldaten durchgehen
        stop = FALSE
        for (year in startYear:endYear) {
            
            if (stop) {
                break
            }
            
            # Das erste Jahr ab dem oben festgelegten Startmonat einlesen.
            # Alle weiteren Jahre ab Januar einlesen.
            if (year == startYear) {
                thisStartMonth <- startMonth
            } else {
                thisStartMonth <- 1
            }
            
            # Das letzte Jahr nur bis zum oben festgelegten Endmonat einlesen.
            # Alle anderen Jahre bis Dezember einlesen.
            if (year == endYear) {
                thisEndMonth <- endMonth
            } else {
                thisEndMonth <- 12
            }
            
            # Jeden Monat des ausgewählten Zeitraumes durchgehen
            for (month in thisStartMonth:thisEndMonth) {
                
                if (stop) {
                    break
                }
                
                # Quell- und Zieldateien für diesen Monat bestimmen
                srcFile <- getSourceFileCallback(pair, year, month)
                targetFileTick <- paste0(cacheBaseTick, year, "-", sprintf("%02d", month), ".rds")
                targetFile1s <- paste0(cacheBase1s, year, "-", sprintf("%02d", month), ".rds")
                targetFile5s <- paste0(cacheBase5s, year, "-", sprintf("%02d", month), ".rds")
                targetFile60s <- paste0(cacheBase60s, year, "-", sprintf("%02d", month), ".rds")
                
                # Quell-Datensatz existiert nicht
                if (!file.exists(srcFile) && !file.exists(targetFileTick)) {
                    # Es sind neue Daten vorhanden, dieser Datensatz fehlt allerdings.
                    if (newDataFound) {
                        cat(paste0(srcFile, " nicht gefunden! Ende.\n"))
                        stop = TRUE
                    }
                    next()
                }
                
                # Neue Daten wurden gefunden
                newDataFound <- TRUE
                tic()
                cat(exchangeName, " ", toupper(pair), " ", sprintf("%02d", month), "/", year, ": ", sep="")
                
                # Tickdaten verarbeiten
                if (file.exists(targetFileTick)) {
                    
                    # Vorhandene Tickdaten dieses Monats einlesen
                    thisDataset <- readRDS(targetFileTick)
                    
                } else {
                    
                    # Tickdaten aus CSV einlesen
                    thisDataset <- parseSourceFileCallback(srcFile)
                    
                    # Tickdaten speichern
                    saveRDS(thisDataset, targetFileTick)
                }
                
                # Statistiken ausgeben
                cat(
                    format(first(thisDataset$Time), format="%d.%m.%Y %H:%M:%S"),
                    "-",
                    format(last(thisDataset$Time), format="%d.%m.%Y %H:%M:%S")
                )
                
                # Auf 1s aggregieren
                if (!file.exists(targetFile1s)) {
                    cat(" - 1s")
                    thisDataset_1s <- thisDataset |>
                        group_by(floor_date(Time, unit = "second")) |>
                        summariseDataCallback()
                    colnames(thisDataset_1s)[1] <- "Time"
                    saveRDS(thisDataset_1s, targetFile1s)
                    
                    # Speicher freigeben
                    rm(thisDataset_1s)
                }
                
                # Auf 5s aggregieren
                if (!file.exists(targetFile5s)) {
                    cat(", 5s")
                    thisDataset_5s <- thisDataset |>
                        group_by(floor_date(Time, unit = "5 seconds")) |>
                        summariseDataCallback()
                    colnames(thisDataset_5s)[1] <- "Time"
                    saveRDS(thisDataset_5s, targetFile5s)
                    
                    # Speicher freigeben
                    rm(thisDataset_5s)
                }
                
                # Auf 60s aggregieren
                if (!file.exists(targetFile60s)) {
                    cat(", 60s")
                    thisDataset_60s <- thisDataset |>
                        group_by(floor_date(Time, unit = "minute")) |>
                        summariseDataCallback()
                    colnames(thisDataset_60s)[1] <- "Time"
                    saveRDS(thisDataset_60s, targetFile60s)
                    
                    # Speicher freigeben
                    rm(thisDataset_60s)
                }
                
                # Auf 1d aggregieren (einzelne Datei für gesamten Datensatz)
                cat(", 1d ")
                thisDataset_daily <- thisDataset |>
                    group_by(floor_date(Time, unit = "1 day")) |>
                    summariseDataCallback()
                colnames(thisDataset_daily)[1] <- "Time"
                dataset_daily <- rbind(dataset_daily, thisDataset_daily)
                saveRDS(dataset_daily, targetFileDaily)
                rm(thisDataset_daily)
                
                # Auf 1 Monat aggregieren (einzelne Datei für gesamten Datensatz)
                cat(", 1mo ")
                thisDataset_monthly <- thisDataset |>
                    group_by(floor_date(Time, unit = "1 month")) |>
                    summariseDataCallback()
                colnames(thisDataset_monthly)[1] <- "Time"
                dataset_monthly <- rbind(dataset_monthly, thisDataset_monthly)
                saveRDS(dataset_monthly, targetFileMonthly)
                
                rm(thisDataset_monthly, thisDataset)
                toc()
            } # loop: month
        } # loop: year
    } # loop: pair
}

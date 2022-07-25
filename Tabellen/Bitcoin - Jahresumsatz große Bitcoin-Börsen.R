# Aufruf durch LaTeX, sonst direkt aus RStudio
fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE

# Nicht aus LaTeX heraus aufrufen, da Daten zwischenzeitlich ohnehin
# nur manuell aktualisiert werden
if (fromLaTeX) {
    stop("Aktualisierung aus LaTeX heraus nicht aktiviert.")
}


# Konfiguration -----------------------------------------------------------
source("Konfiguration/FilePaths.R")

# Grafik (texFile) derzeit nicht genutzt
texFile <- sprintf("%s/Abbildungen/Krypto_Jahresumsatz_nach_Boerse_und_Waehrungspaar.tex",
                   latexOutPath)
asTeX <- FALSE

# Tabellen
templateFiles <- list(
    c(
        sprintf("%s/Tabellen/Templates/Bitcoin_Umsatz_nach_Waehrungspaar_und_Boerse.tex",
                latexOutPath),
        sprintf("%s/Tabellen/Bitcoin_Umsatz_nach_Waehrungspaar_und_Boerse.tex", latexOutPath)
    ),
    c(
        sprintf("%s/Tabellen/Templates/Bitcoin_Umsatz_nach_Waehrungspaar.tex", latexOutPath),
        sprintf("%s/Tabellen/Bitcoin_Umsatz_nach_Waehrungspaar.tex", latexOutPath)
    ),
    c(
        sprintf("%s/Tabellen/Templates/Bitcoin_Boersen_Uebersicht.tex", latexOutPath),
        sprintf("%s/Tabellen/Bitcoin_Boersen_Uebersicht.tex", latexOutPath)
    )
)

# Zeitraum
dateFrom <- "2019-01-01 00:00:00" # Inklusive. Frühestens 01.01.2018
dateTo <- "2022-01-01 00:00:00" # Exklusive
durationInYears <- 3L

# Beispielkurse
sampleExchangeRate_a = 5000 # 5.000 USD
sampleExchangeRate_b = 65000 # 65.000 USD


# Bibliotheken und Hilfsfunktionen laden --------------------------------------
library("fst")
library("data.table")
library("readr")
library("stringr")
library("ggplot2")
library("ggthemes")
source("Funktionen/FormatNumber.R")
source("Funktionen/printf.R")

# Quelldaten
srcsets <- fread("
    Exchange,Pair,SrcFile,InterpolationNeeded
    Bitfinex,BTC/USD,Cache/bitfinex/btcusd/bitfinex-btcusd-daily.fst,0
    Bitfinex,BTC/EUR,Cache/bitfinex/btceur/bitfinex-btceur-daily.fst,0
    Bitfinex,BTC/GBP,Cache/bitfinex/btcgbp/bitfinex-btcgbp-daily.fst,0
    Bitfinex,BTC/JPY,Cache/bitfinex/btcjpy/bitfinex-btcjpy-daily.fst,0
    Bitstamp,BTC/USD,Cache/bitstamp/btcusd/bitstamp-btcusd-daily.fst,0
    Bitstamp,BTC/EUR,Cache/bitstamp/btceur/bitstamp-btceur-daily.fst,0
    Bitstamp,BTC/GBP,Cache/bitstamp/btcgbp/bitstamp-btcgbp-daily.fst,1
    Coinbase Pro,BTC/USD,Cache/coinbase/btcusd/coinbase-btcusd-daily.fst,0
    Coinbase Pro,BTC/EUR,Cache/coinbase/btceur/coinbase-btceur-daily.fst,0
    Coinbase Pro,BTC/GBP,Cache/coinbase/btcgbp/coinbase-btcgbp-daily.fst,0
    Kraken,BTC/USD,Cache/kraken/btcusd/kraken-btcusd-daily.fst,0
    Kraken,BTC/EUR,Cache/kraken/btceur/kraken-btceur-daily.fst,0
    Kraken,BTC/GBP,Cache/kraken/btcgbp/kraken-btcgbp-daily.fst,0
    Kraken,BTC/JPY,Cache/kraken/btcjpy/kraken-btcjpy-daily.fst,0
    Kraken,BTC/CAD,Cache/kraken/btccad/kraken-btccad-daily.fst,0
    Kraken,BTC/CHF,Cache/kraken/btcchf/kraken-btcchf-daily.fst,1
    Kraken,BTC/AUD,Cache/kraken/btcaud/kraken-btcaud-daily.fst,1
")
#Bitstamp BTC/GBP erst ab Mai 2020
#Kraken BTC/CHF erst ab Dezember 2019
#Kraken BTC/AUD erst ab Juni 2020

# Ergebnistabelle
volumeSet <- data.table(
    Exchange = character(),
    Pair = character(),
    Volume = double(),
    Share = double()
)

for (i in seq_len(nrow(srcsets))) {
    srcset <- srcsets[i]
    printf("Lese %s: %s... ", srcset$Exchange, srcset$Pair)
    
    if (!file.exists(srcset$SrcFile)) {
        stop("Datei nicht gefunden!")
    }
    
    dataset <- read_fst(srcset$SrcFile, as.data.table=TRUE)
    printf(
        "%s Datensätze von %s bis %s.\n",
        format.number(nrow(dataset)),
        format(dataset[1, Time], "%d.%m.%Y"),
        format(dataset[.N, Time], "%d.%m.%Y")
    )
    
    # Nach Datum filtern
    dataset <- dataset[dataset$Time >= dateFrom & dataset$Time < dateTo]
    
    # Handelsvolumen berechnen
    volume <- sum(abs(dataset$Amount))
    
    # Annualisieren
    if (srcset$InterpolationNeeded == 1) {
        anzahl_tage_ist <- as.double(Sys.Date() - as.Date(first(dataset$Time)), units="days")
        anzahl_tage_soll <- as.double(as.POSIXct(dateTo) - as.POSIXct(dateFrom), units="days")
        volume <- volume / anzahl_tage_ist * anzahl_tage_soll
    } else {
        volume <- volume / durationInYears
    }
    
    # In Ergebnistabelle speichern
    volumeSet <- rbindlist(list(volumeSet, data.table(
        Exchange = srcset$Exchange,
        Pair = srcset$Pair,
        Volume = volume,
        Share = 0
    )))
    rm(dataset)
}

# Gesamtanteil nach Währung
for (pair in unique(volumeSet$Pair)) {
    totalVolumeThisPair <- sum(volumeSet[volumeSet$Pair == pair]$Volume)
    volumeSet <- rbindlist(list(volumeSet, data.table(
        Exchange = "Total",
        Pair = pair,
        Volume = totalVolumeThisPair,
        Share = 0
    )))
}

# Berechne prozentuale Anteile
for (i in seq_len(nrow(volumeSet))) {
    set <- volumeSet[i]
    totalVolumeThisExchange <- sum(volumeSet[volumeSet$Exchange == set$Exchange]$Volume)
    volumeSet[i]$Share <- set$Volume / totalVolumeThisExchange
}

# Währungspaare nach (geschätzter) Größe absteigend sortieren
volumeSet$Pair <- factor(
    volumeSet$Pair,
    levels=c("BTC/USD", "BTC/EUR", "BTC/JPY", "BTC/GBP", "BTC/CAD", "BTC/CHF", "BTC/AUD")
)

# Tabellen bauen
for (template in templateFiles) {
    
    templateSourceFile <- template[1]
    templateTargetFile <- template[2]
    
    if (!file.exists(templateSourceFile)) {
        stop("templateSource not found.")
    }
    
    cat("Writing", basename(templateTargetFile), "...\n")
    
    # LaTeX-Template lesen
    latexTemplate <- read_file(templateSourceFile)
    
    # Alle Handelspaare durchgehen
    for (i in seq_len(nrow(volumeSet))) {
        set <- volumeSet[i]
        
        # Bitcoin-Volumen (in Tsd.)
        latexTemplate |> str_replace(
            fixed(paste0("{", set$Exchange, ".", set$Pair, ".Volume}")),
            (set$Volume / 1000) |> format.numberWithFixedDigits()
        ) -> latexTemplate
        
        # Volumen in USD (Wechselkurs-Variante a), in Mrd.
        thisVolumeUSD_a <- set$Volume * sampleExchangeRate_a / 1e9
        if (thisVolumeUSD_a > 0.1) {
            thisVolumeUSD_a <- format.numberWithFixedDigits(thisVolumeUSD_a)
        } else {
            # Werte unter 0,1 nicht anzeigen
            thisVolumeUSD_a <- "$<$~0,1"
        }
        latexTemplate |> str_replace(
            fixed(paste0("{", set$Exchange, ".", set$Pair, ".VolumeUSD_a}")),
            thisVolumeUSD_a
        ) -> latexTemplate
        
        # Volumen in USD (Wechselkurs-Variante b), in Mrd.
        thisVolumeUSD_b <- set$Volume * sampleExchangeRate_b / 1e9
        if (thisVolumeUSD_b > 0.1) {
            thisVolumeUSD_b <- format.numberWithFixedDigits(thisVolumeUSD_b)
        } else {
            # Werte unter 0,1 nicht anzeigen
            thisVolumeUSD_b <- "$<$~0,1"
        }
        latexTemplate |> str_replace(
            fixed(paste0("{", set$Exchange, ".", set$Pair, ".VolumeUSD_b}")),
            thisVolumeUSD_b
        ) -> latexTemplate
        
        # Prozentualer Anteil am gesamten Handelsvolumen dieser Börse
        if (set$Share >= 0.1/100) {
            thisShare <- format.percentage(set$Share, digits=1L)
        } else {
            # Kleinere Werte als 0,1 nicht anzeigen
            thisShare <- "$<$~0,1"
        }
        latexTemplate |> str_replace(
            fixed(paste0("{", set$Exchange, ".", set$Pair, ".Share}")),
            thisShare
        ) -> latexTemplate
    }
    
    # Gesamtangaben für alle Börsen
    for (exchange in unique(volumeSet$Exchange)) {
        
        # Gesamtvolumen berechnen
        volumeTotal <- sum(volumeSet$Volume[volumeSet$Exchange == exchange])
        
        # Bitcoin-Volumen (in Mio.)
        latexTemplate |>
            str_replace(
                fixed(paste0("{", exchange, ".TotalVolume}")), 
                format.numberWithFixedDigits(volumeTotal / 1e6)
            ) -> latexTemplate
        
        
        # Umrechnung in USD bei Beispielkurs a)
        latexTemplate |>
            str_replace(
                fixed(paste0("{", exchange, ".TotalVolumeUSD_a}")),
                format.number(round(volumeTotal * sampleExchangeRate_a / 1e9))
            ) -> latexTemplate
        
        # Umrechnung in USD bei Beispielkurs b)
        latexTemplate |>
            str_replace(
                fixed(paste0("{", exchange, ".TotalVolumeUSD_b}")),
                format.number(round(volumeTotal * sampleExchangeRate_b / 1e9))
            ) -> latexTemplate
    }
    
    if (file.exists(templateTargetFile)) {
        Sys.chmod(templateTargetFile, mode="0644")
    }
    latexTemplate <- latexTemplate |> 
        str_replace(
            fixed("{CurrentDate}"),
            trimws(paste0(format(as.POSIXct(dateFrom), "%m/%Y"), " -- ", format(as.POSIXct(dateTo)-1, "%m/%Y")))
        ) |>
        write_file(templateTargetFile)
    
    # Vor versehentlichem Überschreiben schützen
    Sys.chmod(templateTargetFile, mode="0444")
}

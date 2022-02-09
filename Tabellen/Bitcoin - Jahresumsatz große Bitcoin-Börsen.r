# Aufruf durch LaTeX, sonst direkt aus RStudio
fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE

# Nicht aus LaTeX heraus aufrufen, da Daten zwischenzeitlich ohnehin
# nur manuell aktualisiert werden
if (fromLaTeX) {
    stop("Aktualisierung aus LaTeX heraus nicht aktiviert.")
}


# Konfiguration -----------------------------------------------------------
source("Konfiguration/FilePaths.r")

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
sampleExchangeRate_a = 20000 # 20.000 USD
sampleExchangeRate_b = 50000 # 50.000 USD


# Bibliotheken und Hilfsfunktionen laden --------------------------------------
library("fst")
library("data.table")
library("dplyr")
library("readr")
library("stringr")
library("ggplot2")
library("ggthemes")
source("Funktionen/FormatNumber.r")

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
    Exchange=character(),
    Pair=character(),
    Volume=double(),
    Share=double()
)

for (i in seq_len(nrow(srcsets))) {
    srcset <- srcsets[i]
    cat("Reading", srcset$Exchange, srcset$Pair, "\n")
    
    if (!file.exists(srcset$SrcFile)) {
        cat("File not found, skipping.\n")
        warning(paste("File not found:", srcset$SrcFile))
        next()
    }
    
    dataset <- read_fst(srcset$SrcFile, as.data.table=TRUE)
    
    # Nach Datum filtern
    dataset <- dataset[dataset$Time >= dateFrom & dataset$Time < dateTo]
    
    # Handelsvolumen berechnen
    volume <- sum(abs(dataset$Amount))
    
    # Annualisieren
    if (srcset$InterpolationNeeded == 1) {
        anzahl_tage_ist <- as.double(Sys.Date() - as.Date(first(dataset$Time)))
        anzahl_tage_soll <- as.double(as.POSIXct(dateTo) - as.POSIXct(dateFrom))
        volume <- volume / anzahl_tage_ist * anzahl_tage_soll
    } else {
        volume <- volume / durationInYears
    }
    
    # In Ergebnistabelle speichern
    volumeSet <- rbind(volumeSet, data.table(
        Exchange = srcset$Exchange,
        Pair = srcset$Pair,
        Volume = volume,
        Share = 0
    ))
    rm(dataset)
}

# Gesamtanteil nach Währung
for (pair in unique(volumeSet$Pair)) {
    totalVolumeThisPair <- sum(volumeSet[volumeSet$Pair == pair]$Volume)
    volumeSet <- rbind(volumeSet, data.table(
        Exchange = "Total",
        Pair = pair,
        Volume = totalVolumeThisPair,
        Share = 0
    ))
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
                round(volumeTotal * sampleExchangeRate_a / 1e9)
            ) -> latexTemplate
        
        # Umrechnung in USD bei Beispielkurs b)
        latexTemplate |>
            str_replace(
                fixed(paste0("{", exchange, ".TotalVolumeUSD_b}")),
                round(volumeTotal * sampleExchangeRate_b / 1e9)
            ) -> latexTemplate
    }
    
    if (file.exists(templateTargetFile)) {
        Sys.chmod(templateTargetFile, mode="0644")
    }
    latexTemplate <- latexTemplate |> 
        str_replace(fixed("{CurrentDate}"), trimws(format(Sys.Date(), "%B~%Y"))) |>
        write_file(templateTargetFile)
    
    # Vor versehentlichem Überschreiben schützen
    Sys.chmod(templateTargetFile, mode="0444")
}

# Plot ausgeben
if (asTeX) {
    stop("Code-Segment veraltet, Prüfung nötig.")
    source("Konfiguration/TikZ.r")
    cat("Ausgabe in Datei ", texFile, "\n")
    tikz(
        file = texFile,
        width = documentPageWidth,
        height = defaultImageHeight,
        sanitize = TRUE
    )
    
    # Plotten
    # Filter jedes mal erneut prüfen!
    warning("Manually removing non-significant exchange pairs.")
    warning("Check y scale expansion (geom_text must fit)")
    plot <- volumeSet %>%
        filter(
            Exchange != "Total",
            Exchange != "Kraken" | Pair != "BTC/GBP",
            Exchange != "Kraken" | Pair != "BTC/JPY",
            Exchange != "Kraken" | Pair != "BTC/CAD"
        ) %>%
        ggplot(aes(fill=Pair, x=Exchange, y=Volume/1e6)) +
        geom_bar(position = position_dodge2(preserve = "single"), stat="identity") +
        geom_text(
            aes(x=Exchange, y=Volume/1e6, label=Pair, hjust=-.15),
            position=position_dodge2(width=.9, preserve = "single"),
            size=2.5, angle=90
        ) +
        labs(x="\\footnotesize Börse", y="\\footnotesize Jahresumsatz [Mio. BTC]") +
        theme_minimal() +
        theme(
            # legend.position = c(0.85, 0.75),
            # legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            # legend.margin = margin(0, 10, 5, 5),
            # legend.title = element_blank(),
            legend.position = "none",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0)),
            panel.grid.major.x = element_blank()
         ) + 
        scale_x_discrete(
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_continuous(
            #labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",") },
            breaks=seq(0, 30, 1),
            #minor_breaks = NULL,
            # Von c(0, .05) erweitert für Label oberhalb der Balken
            expand = expansion(mult = c(0, .31))
        ) + 
        scale_fill_ptol()
    
    print(plot)
    if (asTeX) {
        dev.off()
    }
    
}

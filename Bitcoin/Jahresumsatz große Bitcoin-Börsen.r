
# Aufruf durch LaTeX, sonst direkt aus RStudio
fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE

# Nicht aus R heraus aufrufen, da Daten zwischenzeitlich ohnehin nur manuell aktualisiert werden
if (fromLaTeX) {
    stop("Aktualisierung aus LaTeX heraus nicht aktiviert.")
}

# Ausgabedateien
# Grafik (texFile) derzeit nicht genutzt
texFile <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Jahresumsatz_nach_Boerse_und_Waehrungspaar.tex"
asTeX <- FALSE
templateFiles <- list(
    c(
        "/Users/fox/Documents/Studium - Promotion/TeX/R/Tabellen/Templates/Bitcoin_Umsatz_nach_Waehrungspaar_und_Boerse.tex",
        "/Users/fox/Documents/Studium - Promotion/TeX/R/Tabellen/Bitcoin_Umsatz_nach_Waehrungspaar_und_Boerse.tex"
    ),
    c(
        "/Users/fox/Documents/Studium - Promotion/TeX/R/Tabellen/Templates/Bitcoin_Umsatz_nach_Waehrungspaar.tex",
        "/Users/fox/Documents/Studium - Promotion/TeX/R/Tabellen/Bitcoin_Umsatz_nach_Waehrungspaar.tex"
    ),
    c(
        "/Users/fox/Documents/Studium - Promotion/TeX/R/Tabellen/Templates/Bitcoin_Boersen_Uebersicht.tex",
        "/Users/fox/Documents/Studium - Promotion/TeX/R/Tabellen/Bitcoin_Boersen_Uebersicht.tex"
    )
)
dateFrom <- "2018-07-01 00:00:00" # Inklusive. Frühestens 01.01.2018
dateTo <- "2021-07-01 00:00:00" # Exklusive

# Beispielkurse
sampleExchangeRate_a = 20000
sampleExchangeRate_b = 50000


# Bibliotheken laden ----------------------------------------------------------
library("fst")
library("data.table")
library("dplyr")
library("readr")
library("stringr")
library("ggplot2")
library("ggthemes")

# Quelldaten
srcsets <- fread("
    Exchange,Pair,SrcFile,InterpolationNeeded
    Bitfinex,BTC/USD,Cache/bitfinex/btcusd/bitfinex-btcusd-daily.fst,0
    Bitfinex,BTC/EUR,Cache/bitfinex/btceur/bitfinex-btceur-daily.fst,0
    Bitfinex,BTC/GBP,Cache/bitfinex/btcgbp/bitfinex-btcgbp-daily.fst,0
    Bitfinex,BTC/JPY,Cache/bitfinex/btcjpy/bitfinex-btcjpy-daily.fst,0
    Bitstamp,BTC/USD,Cache/bitstamp/btcusd/bitstamp-btcusd-daily.fst,0
    Bitstamp,BTC/EUR,Cache/bitstamp/btceur/bitstamp-btceur-daily.fst,0
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
#Kraken BTC/CHF erst ab Dezember 2019
#Kraken BTC/AUD erst ab Juni 2020

# Ergebnistabelle
volumeSet <- data.table(
    Exchange=character(),
    Pair=character(),
    Volume=double(),
    VolumeTsdPretty=double(),
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
    
    dataset <- read_fst(srcset$SrcFile, as.data.table = TRUE)
    
    # Nach Datum filtern
    dataset <- dataset[dataset$Time >= dateFrom,]
    dataset <- dataset[dataset$Time < dateTo,]
    
    # Handelsvolumen berechnen
    volume <- sum(abs(dataset$Amount))
    
    # Annualisieren
    if (srcset$InterpolationNeeded == 1) {
        anzahl_tage_ist <- as.double(Sys.Date() - as.Date(first(dataset$Time)))
        anzahl_tage_soll <- as.double(as.POSIXct(dateTo) - as.POSIXct(dateFrom))
        volume <- volume / anzahl_tage_ist * anzahl_tage_soll
    } else {
        volume <- volume / 2
    }
    
    # In Ergebnistabelle speichern
    volumeSet <- rbind(volumeSet, data.table(
        Exchange=srcset$Exchange,
        Pair=srcset$Pair,
        Volume=volume,
        VolumeTsdPretty=round(volume/1000,1),
        Share=0
    ))
    rm(dataset)
}

# Gesamtanteil nach Währung
for (pair in unique(volumeSet$Pair)) {
    totalVolumeThisPair <- sum(volumeSet[volumeSet$Pair == pair]$Volume)
    volumeSet <- rbind(volumeSet, data.table(
        Exchange="Total",
        Pair=pair,
        Volume=totalVolumeThisPair,
        VolumeTsdPretty=round(totalVolumeThisPair/1000,1),
        Share=0
    ))
}

# Berechne prozentuale Anteile
for (i in seq_len(nrow(volumeSet))) {
    set <- volumeSet[i]
    totalVolumeThisExchange <- sum(volumeSet[volumeSet$Exchange == set$Exchange]$Volume)
    volumeSet[i]$Share <- round(set$Volume / totalVolumeThisExchange * 100, 1)
}

# Währungspaare nach (geschätzter) Größe absteigend sortieren
volumeSet$Pair <- factor(
    volumeSet$Pair,
    levels=c("BTC/USD", "BTC/EUR", "BTC/JPY", "BTC/GBP", "BTC/CAD", "BTC/CHF")
)

# Templates bauen
for (template in templateFiles) {
    
    templateSourceFile <- template[1]
    templateTargetFile <- template[2]
    
    if (!file.exists(templateSourceFile)) {
        stop("templateSource not found.")
    }
    if (file.exists(templateTargetFile)) {
        Sys.chmod(templateTargetFile, mode="0644")
    }
    
    # LaTeX-Templates aktualisieren
    latexTemplate <- read_file(templateSourceFile)
    for (i in seq_len(nrow(volumeSet))) {
        set <- volumeSet[i]
        
        thisVolume <- set$VolumeTsdPretty
        if (thisVolume > 10) {
            thisVolume <- round(thisVolume)
        }
        thisVolume <- prettyNum(thisVolume, big.mark=".", decimal.mark=",")
        
        thisVolumeUSD_a <- set$Volume * sampleExchangeRate_a / 1e9
        if (thisVolumeUSD_a > 10) {
            thisVolumeUSD_a <- round(thisVolumeUSD_a)
        } else {
            thisVolumeUSD_a <- round(thisVolumeUSD_a, 1)
        }
        if (thisVolumeUSD_a > 0.1) {
            thisVolumeUSD_a <- prettyNum(thisVolumeUSD_a, big.mark=".", decimal.mark=",")
        } else {
            thisVolumeUSD_a <- "$<$~0,1"
        }
        
        thisVolumeUSD_b <- set$Volume * sampleExchangeRate_b / 1e9
        if (thisVolumeUSD_b > 10) {
            thisVolumeUSD_b <- round(thisVolumeUSD_b)
        } else {
            thisVolumeUSD_b <- round(thisVolumeUSD_b, 1)
        }
        if (thisVolumeUSD_b > 0.1) {
            thisVolumeUSD_b <- prettyNum(thisVolumeUSD_b, big.mark=".", decimal.mark=",")
        } else {
            thisVolumeUSD_b <- "$<$~0,1"
        }
        
        if (set$Share >= 0.1) {
            thisShare <- prettyNum(set$Share, big.mark=".", decimal.mark=",")
        } else {
            thisShare <- "$<$~0,1"
        }
        latexTemplate <- str_replace(
            latexTemplate, coll(paste0("{", set$Exchange, ".", set$Pair, ".Volume}")), thisVolume
        )
        latexTemplate <- str_replace(
            latexTemplate, coll(paste0("{", set$Exchange, ".", set$Pair, ".VolumeUSD_a}")), thisVolumeUSD_a
        )
        latexTemplate <- str_replace(
            latexTemplate, coll(paste0("{", set$Exchange, ".", set$Pair, ".VolumeUSD_b}")), thisVolumeUSD_b
        )
        latexTemplate <- str_replace(
            latexTemplate, coll(paste0("{", set$Exchange, ".", set$Pair, ".Share}")), thisShare
        )
    }
    for (exchange in unique(volumeSet$Exchange)) {
        volumeTotal <- sum(volumeSet$Volume[volumeSet$Exchange == exchange])
        volumeTotalPretty <- prettyNum(round(volumeTotal / 1000), big.mark=".", decimal.mark=",")
        volumeTotalUSD_a <- volumeTotal * sampleExchangeRate_a # Umrechnung in USD bei Beispielkurs a)
        volumeTotalUSD_a <- prettyNum(round(volumeTotalUSD_a / 1e9), big.mark=".", decimal.mark=",")
        volumeTotalUSD_b <- volumeTotal * sampleExchangeRate_b # Umrechnung in USD bei Beispielkurs b)
        volumeTotalUSD_b <- prettyNum(round(volumeTotalUSD_b / 1e9), big.mark=".", decimal.mark=",")
        
        latexTemplate <- str_replace(
            latexTemplate, coll(paste0("{", exchange, ".TotalVolume}")), volumeTotalPretty
        )
        latexTemplate <- str_replace(
            latexTemplate, coll(paste0("{", exchange, ".TotalVolumeUSD_a}")), volumeTotalUSD_a
        )
        latexTemplate <- str_replace(
            latexTemplate, coll(paste0("{", exchange, ".TotalVolumeUSD_b}")), volumeTotalUSD_b
        )
    }
    
    latexTemplate <- str_replace(latexTemplate, coll("{CurrentDate}"), trimws(format(Sys.Date(), "%B~%Y")))
    write_file(latexTemplate, templateTargetFile)
    # Vor versehentlichem Überschreiben schützen
    Sys.chmod(templateTargetFile, mode="0444")

}
    
# TeX aktivieren
if (asTeX) {
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
            expand = expansion(mult = c(0, .31)) # Von c(0, .05) erweitert für Label oberhalb der Balken
        ) + 
        scale_fill_ptol()
    
    print(plot)
    if (asTeX) {
        dev.off()
    }
    
}

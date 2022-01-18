# Vergleich der 1d/1w/1M-Volatilität von BTCUSD und EURUSD

# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    ####
    # Verwendet in Arbeit:
    # Kapitel: Krypto - Eigenschaften - Ökonomik - Volatilität
    # Label: Krypto_Volatilitaet_BPI
    ####
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    if (!fromLaTeX) {
        cat("Aufruf in RStudio erkannt.\n")
    }
    
    # Konfiguration -----------------------------------------------------------
    asTeX <- fromLaTeX || F # Nur für Testzwecke in RStudio auf F setzen, da sonst keine automatische Aktualisierung
    texFile <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Volatilitaet_BPI.tex"
    outFileTimestamp <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Volatilitaet_BPI_Stand.tex"
    
    # Nur einmal pro Monat neu laden
    if (fromLaTeX && asTeX && file.exists(texFile) && difftime(Sys.time(), file.mtime(texFile), units = "days") < 28) {
        cat("Grafik Volatilitaetsvergleich noch aktuell, keine Aktualisierung.\n")
        return(invisible())
    }
    
    # Bibliotheken laden ------------------------------------------------------
    library("fst")
    library("data.table")
    library("dplyr")
    library("ggplot2")
    library("ggthemes")
    library("scales") # breaks_log
    library("TTR") # Technical Trading Rules
    
    # Berechnungen durchführen ------------------------------------------------
    # Ausgelagert in eigene Datei, um in LaTeX eingebunden zu werden.
    # Anmerkungen:
    # Manuelle Kontrolle:
    # n = 365
    # bpi_mean = mean(btcusd$Rendite[1:n])
    # bpi_partsum = 0
    # for (i in (1:n)) { bpi_partsum = bpi_partsum + (btcusd$Rendite[i] - bpi_mean)^2 }
    # bpi_mean_vola = sqrt(1/(n-2)*bpi_partsum) # <----- Hier n-2?
    # bpi_mean_ann = bpi_mean_vola * sqrt(365)
    
    # N = 365 für Annualisierung, da jeder Tag Handel
    # n = 365 für Periodenlänge
    # Berechnung gegengeprüft mit Daten von blockchain.com / Berentsen2017, S. 261.
    #btcusd$vClose <- volatility(btcusd$Close, n = 365, N = 365)
    
    # Erst ab dem 365. Wert lässt sich annualisierte Vola. berechnen
    #btcusd <- btcusd[365:nrow(btcusd),]
    
    # Bitcoin-Tagesdaten (BTC/USD) aus Bitcoin Price Index laden
    btcusd <- read_fst("Cache/coindesk/bpi-daily-btcusd.fst", as.data.table = TRUE)
    
    # Auf Daten ab 2011 beschränken, da vorher mehrmals keine Preisveränderungen vorliegen
    btcusd <- btcusd[btcusd$Time >= "2011-01-01",]
    
    # Berechnung der Close-to-close-Volatilität mit Paket "TTR"
    # Hier sind es durchgängige Daten, also 7 Werte pro Woche / 28 pro vier Wochen
    zeitspanne <- 28
    btcusd$vClose <- volatility(btcusd$Close, n = zeitspanne, N = 52)
    
    # Begrenze Datensatz auf gültige Werte
    btcusd <- btcusd[zeitspanne:nrow(btcusd),]
    
    # Analog für EUR/USD aus TrueFX-Datensatz
    # Hier sind es 6 Werte pro Woche (Samstag fehlt) / 24 pro vier Wochen
    zeitspanne <- 24
    eurusd <- read_fst("Cache/Dukascopy/dukascopy-daily-eurusd.fst", as.data.table = TRUE)
    eurusd <- eurusd[eurusd$Time >= "2011-01-01",]
    eurusd$vClose <- volatility(eurusd$Close, n = zeitspanne, N = 52)
    eurusd <- eurusd[zeitspanne:nrow(eurusd),]
    
    
    # Verhältnis beider Volatilitäten
    # diffData <-
    #     merge(
    #         btcusd[, c("Time", "vClose")], eurusd[, c("Time", "vClose")],
    #         by="Time",
    #         suffixes=c(".BTCUSD", ".EURUSD")
    #     )
    # diffData$vClose.Diff <- diffData$vClose.BTCUSD / diffData$vClose.EURUSD
    
    # Datensätze zusammenlegen
    plotData <-
        merge(
            btcusd[, c("Time", "vClose")], eurusd[, c("Time", "vClose")],
            by="Time",
            suffixes=c(".BTCUSD", ".EURUSD")
        ) %>% 
        # Mengennotierung BTC/USD: 1 BTC = 10.871 USD
        setnames("vClose.BTCUSD", "BTC/USD") %>%
        # Mengennotierung EUR/USD: 1 EUR = 1,11 USD
        setnames("vClose.EURUSD", "EUR/USD") %>%
        reshape2::melt(id.vars="Time", variable.name="Datensatz", value.name="vClose")
    
    
    # Grafiken erstellen ------------------------------------------------------
    if (asTeX) {
        source("Konfiguration/TikZ.r")
        cat("Ausgabe in Datei ", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 5.5 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    plotAbsVola <- plotData %>%
        ggplot(aes(x=Time, y=vClose, group=Datensatz)) +
        geom_line(aes(color=Datensatz, linetype=Datensatz), size=1) +
        theme_minimal() +
        theme(
            legend.position = "bottom",# c(.9, .88),
            #legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(l=-5),
            # TODO Testen (aus: Energieverbrauch.r):
            #legend.text = element_text(margin = margin(r = 15)),
            legend.title = element_blank(),
            axis.title.x = element_blank(),#element_text(margin = margin(t = 5), size=9),
            axis.title.y = element_text(size=9, margin=margin(r=10)),
            panel.grid.minor.y = element_line(size = .4),
            panel.grid.major.y = element_line(size = .4)
        )+
        scale_x_date(
            date_breaks="1 year",
            minor_breaks=NULL,
            date_labels="%Y",
            expand = expansion(mult = c(0, 0))
        ) +
        scale_y_log10(
            labels = function(x) { paste0(prettyNum(x*100, big.mark=".", decimal.mark=","), "\\,%") },
            limits = c(NA, 3e0),
            breaks = breaks_log(9),
            minor_breaks = breaks_log(19)
            #expand = expansion(mult = c(.1, .1))
        ) + 
        scale_color_ptol() +
        labs(x="Datum", y="Annualisierte Volatilität")
    print(plotAbsVola)
    
    if (asTeX) {
        dev.off()
    }
    
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    
    return(invisible())
})()

# Vergleich der annualisierten Volatilität von BTCUSD und EURUSD

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
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Krypto_Volatilitaet_BPI.tex", latexOutPath)
    outFileVolaRangeBTCUSD <- sprintf("%s/Daten/Volatilitaet_BTCUSD_Bereich.tex", latexOutPath)
    outFileVolaRangeEURUSD <- sprintf("%s/Daten/Volatilitaet_EURUSD_Bereich.tex", latexOutPath)
    outFileTimestamp <- sprintf("%s/Abbildungen/Krypto_Volatilitaet_BPI_Stand.tex", latexOutPath)
    
    # Nur für Testzwecke in RStudio auf F setzen, da sonst keine automatische Aktualisierung
    plotAsLaTeX <- fromLaTeX || TRUE
    
    # Nur einmal pro Monat neu laden
    if (
        fromLaTeX && plotAsLaTeX && file.exists(texFile) &&
        difftime(Sys.time(), file.mtime(texFile), units = "days") < 28
    ) {
        cat("Grafik Volatilitaetsvergleich noch aktuell, keine Aktualisierung.\n")
        return(invisible())
    }
    
    
    # Bibliotheken laden ------------------------------------------------------
    source("Funktionen/FormatNumber.R")
    library("data.table")
    library("ggplot2")
    library("ggthemes")
    library("scales") # breaks_log
    
    
    # Berechnungen durchführen. Ausgelagert in eigene Datei, um in LaTeX eingebunden zu werden.
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
    #btcusd <- btcusd[365:nrow(btcusd)]
    
    source("Grafiken/Vergleich Volatilität BTCUSD und EURUSD.Standalone.R", local=TRUE)
    
    # Auf relevante Spalten reduzieren, Datensatz ergänzen, Zeit vereinheitlichen
    btcusd <- btcusd[, c("Time", "vClose")]
    btcusd[,Datensatz:="BTC/USD"]
    btcusd[,Time:=as.POSIXct(Time)]
    
    eurusd <- eurusd[, c("Time", "vClose")]
    eurusd[,Datensatz:="EUR/USD"]
    
    plotData <- rbindlist(list(btcusd, eurusd))
    
    # Grafiken erstellen ------------------------------------------------------
    if (plotAsLaTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei ", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 5.5 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    plotAbsVola <- ggplot(plotData, aes(x=Time, y=vClose, group=Datensatz)) +
        geom_line(aes(color=Datensatz, linetype=Datensatz), linewidth=1) +
        theme_minimal() +
        theme(
            legend.position = "bottom",
            legend.margin = margin(t=-5),
            legend.text = element_text(margin = margin(r = 15)),
            legend.title = element_blank(),
            axis.title.x = element_text(margin = margin(t = 5), size=9),
            axis.title.y = element_text(size=9, margin=margin(r=10)),
            panel.grid.minor.y = element_line(size = .4),
            panel.grid.major.y = element_line(size = .4)
        ) +
        scale_x_datetime(
            date_breaks = "1 year",
            minor_breaks = NULL,
            date_labels = "%Y",
            expand = expansion(mult = c(0, 0))
        ) +
        scale_y_log10(
            labels = function(x) { paste0(format.percentage(x, 0L), "\\,%") },
            limits = c(NA, 3e0),
            breaks = breaks_log(9),
            minor_breaks = breaks_log(19)
        ) + 
        scale_color_ptol() +
        labs(x="Datum", y="Annualisierte Volatilität")
    print(plotAbsVola)
    
    if (plotAsLaTeX) {
        dev.off()
    }
    
    cat(
        trimws(format(min(last(btcusd$Time), last(eurusd$Time)), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    
    cat(
        "zwischen ", 
        round(min(btcusd$vClose)*100, 0), "\\,\\%",
        " und ",
        round(max(btcusd$vClose)*100, 0), "\\,\\%",
        "%",
        
        file = outFileVolaRangeBTCUSD,
        sep = ""
    )
    cat(
        "zwischen ", 
        round(min(eurusd$vClose)*100, 0), "\\,\\%",
        " und ",
        round(max(eurusd$vClose)*100, 0), "\\,\\%",
        "%",
        
        file = outFileVolaRangeEURUSD,
        sep = ""
    )
    
    return(invisible())
})()

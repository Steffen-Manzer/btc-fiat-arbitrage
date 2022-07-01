####
# Verwendet in Arbeit:
# Kapitel: 4.2.1 Charakteristik freier Wechselkurse
# Label: Grafik:Preisbildung_Devisen_HistorischeKurse
####

# Nutzt manuell abgerufene Daten, Aktualisierung über:
# Datenanalyse/Crawler/Dukascopy/get-ohlc.sh


# Funktionen und Bibliotheken laden -------------------------------------------
library("data.table")
library("ggplot2")
library("ggthemes")
library("cowplot") # plot_grid
source("Funktionen/FormatCurrencyPair.R")
source("Funktionen/printf.R")
source("Konfiguration/FilePaths.R")


# Konfiguration -----------------------------------------------------------
asTeX <- TRUE # Ausgabe als TeX-Dokument oder in RStudio direkt
texFile <- sprintf(
    "%s/Abbildungen/Markteffizienz_Devisen_HistorischeKurse.tex",
    latexOutPath
)
outFileTimestamp <- sprintf(
    "%s/Abbildungen/Markteffizienz_Devisen_HistorischeKurse_Stand.tex",
    latexOutPath
)


# Daten einlesen --------------------------------------------------------------
dataPathBase <- "Daten/dukascopy-ohlc/"

pairs <- c("EURUSD", "USDJPY", "GBPUSD", "USDCHF")
plotData <- list()

# Einzelne Daten einlesen
for (pair in pairs) {
    cat(paste0("Lese ", pair, "...\n"))
    
    files <- list.files(dataPathBase, pattern=paste0("^", pair |> tolower()), full.names=TRUE)
    if (length(files) != 1) {
        stop(sprintf("Konnte Quelldatei für %s nicht bestimmen.", pair))
    }
    
    dataset <- fread(files[1])
    
    # Datensatz beschreiben
    dataset$Datensatz <- pair
    
    # Zeitstempel
    dataset$timestamp <- as.Date(as.POSIXct(dataset$timestamp/1000, origin = "1970-01-01"))
    
    # Datum einheitlich begrenzen. Historische EURUSD-Kurse nicht ganz klar, daher begrenzen
    if (pair == "EURUSD") {
        dataset <- dataset[dataset$timestamp >= "1999-01-01"]
    } else {
        dataset <- dataset[dataset$timestamp >= "1990-01-01"] # Vormals 1987 (1/2)
    }
    
    # Beispiel GBPUSD:
    # 1987-2002: ca. 258 Handeltage
    # Seit Mitte 2003: 313 Handelstage
    # Ausnahmen:
    # 1998 (254)
    # 1999 (231)
    # 2000 (252)
    # 2001 (253)
    # 2003 (296)
    #dataset$vClose <- volatility(dataset$Close, n=312, N=312)
    
    # Auf sinnvolle Daten (für Volatilität) beschränken
    #dataset <- dataset[312:nrow(dataset)]
    
    # Tagesschlusskurse
    plot <- ggplot(dataset, aes(x=timestamp, y=close, group=Datensatz)) +
        geom_line(aes(color=Datensatz)) +
        theme_minimal() +
        theme(
            legend.position = "none",
            plot.title = element_text(size = 8, hjust = 0.5),
            axis.title.x = element_blank(),
            axis.title.y = element_text(size=7) #element_blank()
        ) +
        scale_x_date(
            date_breaks="5 years",
            minor_breaks="1 year",
            date_labels="%Y",
            limits = c(as.Date("1990-01-01"), NA), # Vormals 1987 (2/2)
            expand = expansion(mult = 0)
        ) +
        scale_y_continuous(
            labels = function(x) { paste0(prettyNum(x, big.mark=".", decimal.mark=",")) },
            expand = expansion(mult = c(.1, .1))
        ) + 
        scale_color_ptol() +
        labs(title=format.currencyPair(pair)) +
        ylab(substr(pair, 4, 6))
    
    plotData <- c(plotData, list(plot))
}


# Grafiken erstellen ------------------------------------------------------
if (asTeX) {
    source("Konfiguration/TikZ.R")
    cat("Ausgabe in Datei ", texFile, "\n")
    tikz(
        file = texFile,
        width = documentPageWidth,
        height = 7 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}

print(plot_grid(plotlist=plotData, ncol = 2L, align = "hv"))

if (asTeX) {
    dev.off()
}

cat(
    trimws(format(Sys.time(), "%B %Y")), "%",
    file = outFileTimestamp,
    sep = ""
)

#' Aufruf aus LaTeX heraus via \executeR{...}
#' Besonderheiten gegenüber normalen Skripten:
#' - setwd + source(.Rprofile)
#' - Caching, da 3x pro Kompilierung aufgerufen
#'
#' Verwendet in Arbeit:
#' Automatische Aktualisierung derzeit nicht aktiv - Prüfen
#' Kapitel: Empirie - Wechselkurse - Eigenschaften und Aufbereitung
#' Label: Grafik:Effizienz_Devisen_Charakteristik

# Derzeit nicht verwendet.

# Minimalpakete laden ---------------------------------------------------------
source("Funktionen/printf.R")
source("Konfiguration/FilePaths.R")

# Aufruf durch LaTeX, sonst direkt aus RStudio
fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
if (!fromLaTeX) {
    printf("Aufruf in RStudio erkannt.\n")
} else {
    stop("Keine Aktualisierung aus LaTeX heraus aktiv!")
}


# Konfiguration ---------------------------------------------------------------
force <- F # Nur für Testzwecke: Datei auch erstellen, wenn noch kein Update nötig ist
asTeX <- fromLaTeX # Ausgabe als TeX-Dokument oder in RStudio direkt
texFile <- sprintf("%s/Abbildungen/Markteffizienz_Devisen_Charakteristik.tex",
                   latexOutPath)
outFileTimestamp <- sprintf("%s/Abbildungen/Markteffizienz_Devisen_Charakteristik_Stand.tex",
                            latexOutPath)


# Nur einmal pro Monat neu laden ----------------------------------------------
if (
    !force && fromLaTeX && asTeX &&
    file.exists(texFile) && difftime(Sys.time(), file.mtime(texFile), units = "days") < 28
) {
    cat("Grafik Wechselkurscharakteristik noch aktuell, keine Aktualisierung.\n")
    return()
}


# Bibliotheken laden ----------------------------------------------------------
library("fst")
library("data.table")
library("ggplot2")
library("ggthemes")
library("gridExtra")
library("TTR") # Technical Trading Rules -> volatility


# Daten aufbereiten -----------------------------------------------------------
dataPathBase <- "Cache/dukascopy/" # Dateinamen: eurusd/dukascopy-eurusd-daily.fst
currencyPairs <- c("AUDUSD", "EURUSD", "GBPUSD", "USDCAD", "USDCHF", "USDJPY")
plotData <- data.table()


# Einzelne Daten einlesen
for (pair in currencyPairs) {
    dataset <- read_fst(
        paste0(dataPathBase, tolower(pair), "/dukascopy-", tolower(pair), "-daily.fst"),
        as.data.table = TRUE
    )
    
    # Auf Zeitraum ab 2014 beschränken
    #dataset <- dataset[dataset$Time >= "2014-01-01", ]
    
    # Datensatz beschreiben
    dataset$Datensatz <- pair
    
    # Volatilität berechnen: Annualisiert = 313 Tage gegenüber 365 Handelstagen bei Bitcoin oder 260 Tagen bei Aktien!
    dataset$vClose <- volatility(dataset$Close, n=312, N=312)
    
    # Auf sinnvolle Daten (für Volatilität) beschränken
    dataset <- dataset[312:nrow(dataset)]
    
    # Nur die notwendigen Spalten behalten: Datensatz, Schlusskurs, Tagesrendite, Volatilität
    dataset <- dataset[, c("Time", "Datensatz", "Close", "Rendite", "vClose")]
    
    # Datensatz in Ergebnisliste schreiben
    plotData <- rbind(plotData, dataset)
    
    # Umgebung bereinigen
    rm(dataset)
}


# Grafiken erstellen ----------------------------------------------------------
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

# Volatilität
plotVola <-
    ggplot(
        # Aus Gründen der Übersichtlichkeit auf letzte fünf Jahre beschränken
        plotData[Time >= "2014-01-01"],
        aes(x=Time, y=vClose, group=Datensatz)
    ) +
    geom_line(aes(color=Datensatz, linetype=Datensatz), linewidth=1) +
    theme_minimal() +
    theme(
        legend.position = "bottom",
        legend.margin = margin(0, 15, 0, -15),
        legend.title = element_blank(),
        plot.title = element_text(hjust = 0.5),
        axis.title.x = element_blank(),
        axis.title.y = element_blank()
    ) +
    scale_x_date(
        date_breaks="1 year",
        minor_breaks=NULL,
        date_labels="%Y",
        expand = expansion(mult = 0)
        #expand = expansion(mult = c(.02, .02))
    ) +
    scale_y_continuous(
        labels = function(x) { paste0(prettyNum(x*100, big.mark=".", decimal.mark=","), "\\,%") },
        expand = expansion(mult = c(.1, .1))
    ) + 
    scale_color_ptol() +
    labs(title="\\footnotesize Annualisierte Volatilität")


# Renditedichten
plotRendite <- ggplot(plotData, aes(y=Rendite)) +
    geom_density(aes(color=Datensatz, linetype=Datensatz), size=1) +
    #geom_histogram(aes(fill=Datensatz, linetype=Datensatz), binwidth = 5e-4) + 
    facet_wrap(vars(Datensatz)) +
    theme_minimal() +
    theme(
        legend.position = "none",
        plot.title = element_text(hjust = 0.5),
        axis.title.x = element_blank(),
        axis.title.y = element_blank()
    ) +
    scale_x_discrete(expand=c(0,0)) +
    scale_y_continuous(
        breaks = seq(-3e-2, 3e-2, by=1e-2),
        labels = function(n) { prettyNum(n, decimal.mark = ",") },
        limits = c(-3e-2, 3e-2)
    ) +
    scale_fill_ptol() +
    labs(title="\\footnotesize Renditedichteverteilung")

grid.arrange(plotVola, plotRendite, nrow=1)

if (asTeX) {
    dev.off()
}

cat(
    trimws(format(Sys.time(), "%B %Y")), "%",
    file = outFileTimestamp,
    sep = ""
)

#' Erzeugt eine grafische Übersicht über die unterschiedlichen
#' Darstellungsarten, jeweils ohne Intervalle:
#' 
#' 1. Minimum/Maximum/arith. Mittel
#' 2. Nur arithmetisches Mittel
#' 3. Q1/Q3/Median


# Funktionen und Bibliotheken laden -------------------------------------------
source("Raumarbitrage/Bitcoin - Auswertung nach Börsen gruppiert.R")
source("Konfiguration/TikZ.R")


# Konfiguration ---------------------------------------------------------------

# LaTeX
plotAsLaTeX <- TRUE
texFileMinMaxMean <- sprintf(
    "%s/Abbildungen/Raumarbitrage/Methodik_Darstellungsarten_MinMaxMean.tex",
    latexOutPath
)
texFileMean <- sprintf(
    "%s/Abbildungen/Raumarbitrage/Methodik_Darstellungsarten_Mean.tex",
    latexOutPath
)
texFileQ1Q3Median <- sprintf(
    "%s/Abbildungen/Raumarbitrage/Methodik_Darstellungsarten_Q1Q3Median.tex",
    latexOutPath
)

# Beispieldaten und Achsenbeschriftung
pair <- "btcusd"
threshold <- 1L
plotTextPrefix <- "\\footnotesize "
plotXLab <- "Datum"
plotYLab <- "Preisabweichung"


# Rohdaten laden --------------------------------------------------------------
if (!exists("priceDifferences")) {
    comparablePrices <- loadComparablePricesByCurrencyPair(pair, threshold)
    
    # Monatsdaten aggregieren
    priceDifferences <- aggregatePriceDifferences(comparablePrices, "1 month")
    
    # Speicher freigeben
    rm(comparablePrices)
    gc()
}


# Grafiken erstellen ----------------------------------------------------------

# Median
p_diff_median <- plotAggregatedPriceDifferencesByTime(priceDifferences, removeGaps = FALSE)
if (plotAsLaTeX) {
    tikz(
        file = texFileQ1Q3Median,
        width = documentPageWidth,
        height = 4 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}
print(p_diff_median)
if (plotAsLaTeX) {
    dev.off()
}


# Min/Max/Mean
p_diff_minmaxmean <- ggplot(priceDifferences) +
    geom_ribbon(aes(x=Time, ymin=0,ymax=Max),fill="grey70") +
    geom_line(aes(x=Time, y=Mean, color="1", linetype="1"), size=.5) + 
    theme_minimal() +
    theme(
        legend.position = "none",
        plot.title.position = "plot",
        axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
        axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
    ) +
    scale_x_datetime(expand=expansion(mult=c(.01, .03))) +
    scale_y_continuous(labels=function(x) paste(format.number(x * 100), "%")) +
    scale_color_ptol() +
    scale_fill_ptol() +
    labs(
        x = paste0(plotTextPrefix, plotXLab),
        y = paste0(plotTextPrefix, plotYLab)
    )

if (plotAsLaTeX) {
    tikz(
        file = texFileMinMaxMean,
        width = documentPageWidth,
        height = 4 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}
print(p_diff_minmaxmean)
if (plotAsLaTeX) {
    dev.off()
}

# Mean
p_diff_mean <- ggplot(priceDifferences) + 
    geom_line(aes(x=Time, y=Mean, color="1", linetype="1"), size=.5) + 
    theme_minimal() +
    theme(
        legend.position = "none",
        plot.title.position = "plot",
        axis.title.x = element_text(margin=margin(t=5, r=0, b=0, l=0)),
        axis.title.y = element_text(margin=margin(t=0, r=10, b=0, l=0))
    ) +
    scale_x_datetime(expand=expansion(mult=c(.01, .03))) +
    scale_y_continuous(labels=function(x) paste(format.number(x * 100), "%")) +
    scale_color_ptol() +
    scale_fill_ptol() +
    labs(
        x = paste0(plotTextPrefix, plotXLab),
        y = paste0(plotTextPrefix, plotYLab)
    )

if (plotAsLaTeX) {
    tikz(
        file = texFileMean,
        width = documentPageWidth,
        height = 4 / 2.54, # cm -> Zoll
        sanitize = TRUE
    )
}
print(p_diff_mean)
if (plotAsLaTeX) {
    dev.off()
}

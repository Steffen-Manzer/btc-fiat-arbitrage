
(function() {
    
    ####
    # Verwendet in Arbeit:
    # Kapitel Krypto - Eigenschaften - Ökonomik - Deflation 
    # Label: Krypto_Bitcoin_Umlauf
    ####
    library("data.table")
    library("dplyr")
    library("ggplot2")
    library("ggthemes")
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Krypto_Rechnerische_Anzahl_Bitcoins_im_Umlauf.tex", latexOutPath)
    plotAsLaTeX <- TRUE
    
    # Ausgabe auf maximale Bitcoin-Länge (7 vor + 8 nach) einstellen
    #options(digits=15)
    
    # Daten berechnen ---------------------------------------------------------
    finalSum <- 20999999.9769
    numTotal <- 0
    numAvailable <- 0
    reward <- 50
    halvingBlocks <- 210000
    numBlocks <- 0
    results <- data.table(
        block = numeric(), 
        bitcoins = numeric(),
        bitcoinsMinusAttrition = numeric(),
        reward = numeric(), 
        percentage = numeric()
    )
    results <- rbind(results, data.table(
        block = 0,
        bitcoins = 0,
        bitcoinsMinusAttrition = 0,
        reward = reward,
        percentage = 0
    ))
    #for (i in 0:32) {
    for (i in 0:7) {
        currentRewardEightDigits <- floor(reward * 1e8 * 2^-i)
        numBlocks <- numBlocks + halvingBlocks
        numTotal <- numTotal + (halvingBlocks * currentRewardEightDigits)/1e8
        numAvailable <- numTotal - (halvingBlocks * 2^(i*.5))
        results <- rbind(results, data.table(
            block = numBlocks,
            bitcoins = numTotal,
            bitcoinsMinusAttrition = numAvailable,
            reward = currentRewardEightDigits/1e8,
            percentage = numTotal / finalSum
        ))
    }
    results <<- results
    
    
    # Grafiken erstellen ------------------------------------------------------
    if (plotAsLaTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei", texFile, "\n")
        tikz(
            file = paste0(texFile),
            width = documentPageWidth,
            #height = 1.5, # Zoll
            height = 1.8,
            sanitize = TRUE
        )
    }
    
    # Letzte Zahl gepunktet verbinden und nicht in Skala anzeigen
    numResults <- nrow(results)
    lastResults <- results[(numResults-1):numResults]
    results <- results[-nrow(results)]
    
    # Damit der Abstand nach hinten nicht zu groß ist: nur halben Verlauf anzeigen
    lastVisible <- tail(results, 1)
    lastResults$block <- lastVisible$block + (lastResults$block - lastVisible$block) / 2
    lastResults$bitcoins <- lastVisible$bitcoins + (lastResults$bitcoins - lastVisible$bitcoins) / 2
    
    plot <- results %>%
        ggplot(aes(x=block, y=bitcoins)) +
        geom_line(aes(y=bitcoinsMinusAttrition, color="Produktion abzüglich Verluste"), size = 1) +
        geom_line(data=lastResults, aes(x=block, y=bitcoinsMinusAttrition, color="Produktion abzüglich Verluste"), linetype="dotted", size = 1) +
        geom_line(aes(color="Insgesamt produziert"), size = 1) +
        geom_line(data=lastResults, aes(x=block, y=bitcoins, color="Insgesamt produziert"), linetype="dotted", size = 1) +
        scale_x_continuous(
            breaks = results$block,
            minor_breaks = NULL,
            labels = function(x) { paste(prettyNum(x, big.mark=".", decimal.mark=",", scientific=F)) },
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_continuous(
            labels = function(x) { paste(prettyNum(x, big.mark=".", decimal.mark=",", scientific=F)) },
            breaks = c(0, 10e6, 20e6),
            minor_breaks = c(5e6, 15e6),
            #limits = c(0, 21e6),
            expand = expansion(mult = c(.1, .1))
        ) + 
        theme_minimal() +
        theme(
            #legend.position = "none",
            legend.position = c(0.8, 0.35),
            legend.background = element_rect(fill = "white", size = 0.2, linetype = "solid"),
            legend.margin = margin(0, 15, 5, 5),
            legend.title = element_blank(),
            axis.title.x = element_text(size = 9, margin = margin(t = 10)),
            axis.title.y = element_text(size = 9, margin = margin(r = 14)),
        ) +
        scale_color_ptol() + 
        labs(x="Blockhöhe", y="Bitcoin-Menge")
    
    print(plot)
    if (plotAsLaTeX) {
        dev.off()
    }
    
})()

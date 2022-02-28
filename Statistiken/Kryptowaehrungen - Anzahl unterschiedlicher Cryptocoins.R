# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - Caching, da 3x pro Kompilierung aufgerufen
# - Keine Umlaute im Dateinamen

(function() {
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    outFile <- sprintf("%s/Daten/CryptoCoin_Anzahl.tex", latexOutPath)
    outFileLarge <- sprintf("%s/Daten/CryptoCoin_Anzahl_GrosseWaehrungen.tex", latexOutPath)
    outFileTimestamp <- sprintf("%s/Daten/CryptoCoin_Anzahl_Stand.tex", latexOutPath)
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Nur einmal pro Monat neu laden
    if (fromLaTeX && file.exists(outFile) && difftime(Sys.time(), file.mtime(outFile), units = "days") < 28) {
        cat("Daten noch aktuell, keine Aktualisierung der CryptoCoin-Anzahl\n")
        return()
    }
    
    
    # Bibliotheken und Hilfsfunktionen laden ----------------------------------
    source("Funktionen/R_in_LaTeX_Warning.R")
    source("Konfiguration/CoinMarketCap_API_KEY.R")
    library("data.table")
    library("rjson")
    
    
    # Berechnungen ------------------------------------------------------------
    # Anzahl gesamter Währungen
    # API-Doku:
    # https://coinmarketcap.com/api/documentation/v1/#operation/getV1GlobalmetricsQuotesLatest
    sourceURL <- "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"
    
    failed <- FALSE
    tryCatch(
        {
            apiResponse <- system2("curl", c(
                paste0('-H "X-CMC_PRO_API_KEY: ', coinMarketCap_API_Key, '"'),
                '-H "Accept: application/json"',
                '-G',
                sourceURL
            ), stdout = TRUE, timeout = 60)
            
            if (length(apiResponse) < 10) {
                simpleError("Invalid response.")
            }
            
            apiResponse <- do.call(paste0, as.list(apiResponse))
            apiResponse <- fromJSON(apiResponse)
            
            # Prüfe Validität
            if (length(apiResponse) != 2) {
                simpleError("Invalid response length.")
            }
            if (apiResponse$status$error_code > 0) {
                simpleError(apiResponse$status$error_message)
            }
            
            apiResponse <<- apiResponse
            
        },
        error = function(err) {
            failed <<- TRUE
            warning(err)
        }
    )
    
    if (failed) {
        cat(latexWarning("Konnte Cryptocoin-Anzahl nicht lesen!"), file = outFile)
        return()
    }
    
    numCurrencies <- apiResponse$data$active_cryptocurrencies # length(apiResponse$data)
    # Alte Variante: Anzahl = Abfragelimit, ggf. mehr -> Manuell!
    # if (numCurrencies == 5000) {
    #     cat(latexWarning("Konnte Cryptocoin-Anzahl nicht lesen, Anzahl ueber Limit!"), file = outFile)
    #     return()
    # }
    
    # Auf nächste 100 abrunden
    numCurrencies <- floor(numCurrencies / 100) * 100
    numCurrencies <- prettyNum(numCurrencies, big.mark=".", decimal.mark=",")
    
    
    
    # Marktkapitalisierung > 1 Mrd. USD
    sourceURL <- "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    
    # Keine " in diesem String
    sourceParams <- "cryptocurrency_type=coins&sort=market_cap&sort_dir=desc&start=1&limit=200&aux=cmc_rank"
    
    failed <- FALSE
    tryCatch(
        {
            apiResponse <- system2("curl", c(
                paste0('-H "X-CMC_PRO_API_KEY: ', coinMarketCap_API_Key, '"'),
                '-H "Accept: application/json"',
                paste0('-d "', sourceParams, '"'),
                '-G',
                sourceURL
            ), stdout = TRUE, timeout = 60)
            
            if (length(apiResponse) < 10) {
                simpleError("Invalid response.")
            }
            
            apiResponse <- do.call(paste0, as.list(apiResponse))
            apiResponse <- fromJSON(apiResponse)
            
            # Prüfe Validität
            if (length(apiResponse) != 2) {
                simpleError("Invalid response length.")
            }
            if (apiResponse$status$error_code > 0) {
                simpleError(apiResponse$status$error_message)
            }
            
            apiResponse <<- apiResponse
            
        },
        error = function(err) {
            failed <<- TRUE
            warning(err)
        }
    )
    
    if (failed) {
        cat(latexWarning("Konnte Cryptocoin-Anzahl nicht lesen!"), file = outFile)
        return()
    }
    
    numCurrenciesAbove1Billion <- 0
    for (currency in apiResponse$data) {
        if (currency$quote$USD$market_cap < 1e9) {
            break
        }
        numCurrenciesAbove1Billion = numCurrenciesAbove1Billion + 1
    }
    
    if (numCurrenciesAbove1Billion > 12) {
        numCurrenciesAbove1Billion <- prettyNum(numCurrenciesAbove1Billion, big.mark=".", decimal.mark=",")
    } else {
        words <- c('null', 'eine', 'zwei', 'drei', 'vier',
                   'fünf', 'sechs', 'sieben', 'acht', 'neun',
                   'zehn', 'elf', 'zwölf')
        numCurrenciesAbove1Billion <- words[numCurrenciesAbove1Billion + 1]
    }
    
    
    cat(numCurrencies, "%", file = outFile, sep="")
    cat(numCurrenciesAbove1Billion, "%", file = outFileLarge, sep="")
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    cat("Cryptocoinanzahl von ", numCurrencies, " in Datei ", outFile, " geschrieben.\n")
    
})()

(function() {
    
    # Beispieldaten via Bitstamp-API 28.01.2019, ca. 15:30
    # {  
    #     "high":"3561.65",
    #     "last":"3412.67",
    #     "timestamp":"1548685663",
    #     "bid":"3412.67",
    #     "vwap":"3458.64",
    #     "volume":"8993.43947678",
    #     "low":"3357.30",
    #     "ask":"3414.70",
    #     "open":"3532.23"
    # }
    # 
    # Bid: 3.412,67 USD
    # Ask: 3.414,70 USD
    # Mittelkurs: 3.413,685 USD
    # 
    # Ask-Bid: 2,03 USD = 0,0595 % (des Mittelkurses)
    # Rekonstruktion eines Nährungswertes:
    # Bid = Mittelkurs * (1 - Spread% / 100 / 2)
    # Ask = Mittelkurs * (1 + Spread% / 100 / 2)
    #
    #
    # Zum Vergleich: EUR/USD am 31.12.2018, 21:46:00:
    # Bid: 1,14604 USD
    # Ask: 1,14614 USD
    # Mittelkurs: 1.14609 USD
    # 
    # Ask-Bid: 1e-04 USD = 0,0001 USD = 0,01 US-Cent = 0,0087 %
    
    # Hilfsfunktion
    '%nin%' <- Negate('%in%')
    library("data.table")
    library("dplyr")
    library("ggplot2")
    library("ggthemes")
    
    # #### Annahmen
    spread_estimate_btceur = 0.175 # In Prozent
    spread_estimate_btcusd = 0.075 # In Prozent
    
    # Daten laden
    cat("Lade Daten...\n")
    cat("** TODO ** Rewrite für nicht-aggregierte Bitcoin-Daten.\n")
    return()
    bitstampEUR <- readRDS("Cache/bitstamp-bcc/bitstamp-bcc-tick-btceur.rds")
    bitstampUSD <- readRDS("Cache/bitstamp-bcc/bitstamp-bcc-tick-btcusd.rds")
    truefxEURUSD <- readRDS("Cache/TrueFX/truefx-60s-eurusd.rds")
    
    cat("Anzahl Datensätze:\n")
    cat("   BTC/EUR:", nrow(bitstampEUR), "\n")
    cat("   BTC/USD:", nrow(bitstampUSD), "\n")
    cat("   EUR/USD:", nrow(truefxEURUSD), "\n")
    
    # Filtere Liquidität: Mindestens 5x pro Minute gehandelt
    # Sinnvoll??
    cat("Reduziere um wenig liquide Datensätze (Handel weniger als 5x pro Minute)\n")
    bitstampEUR <- bitstampEUR[bitstampEUR$NumDatasets >= 5,]
    bitstampUSD <- bitstampUSD[bitstampUSD$NumDatasets >= 5,]
    truefxEURUSD <- truefxEURUSD[truefxEURUSD$NumDatasets >= 5,]
    cat("Anzahl Datensätze:\n")
    cat("   BTC/EUR:", nrow(bitstampEUR), "\n")
    cat("   BTC/USD:", nrow(bitstampUSD), "\n")
    cat("   EUR/USD:", nrow(truefxEURUSD), "\n")
    
    # Auf gemeinsame Daten beschränken
    minDate <- max(min(bitstampEUR$Time), min(bitstampUSD$Time), min(truefxEURUSD$Time))
    maxDate <- min(max(bitstampEUR$Time), max(bitstampUSD$Time), max(truefxEURUSD$Time))
    
    cat("Reduziere auf gemeinsamen Datensatz: von", format(minDate), "bis", format(maxDate), "\n")
    bitstampEUR <- bitstampEUR[bitstampEUR$Time >= minDate & bitstampEUR$Time <= maxDate,]
    bitstampUSD <- bitstampUSD[bitstampUSD$Time >= minDate & bitstampUSD$Time <= maxDate,]
    truefxEURUSD <- truefxEURUSD[truefxEURUSD$Time >= minDate & truefxEURUSD$Time <= maxDate,]
    
    bitstampUSD <- bitstampUSD[bitstampUSD$Time %nin% setdiff(bitstampUSD$Time, bitstampEUR$Time),]
    bitstampUSD <- bitstampUSD[bitstampUSD$Time %nin% setdiff(bitstampUSD$Time, truefxEURUSD$Time),]
    
    bitstampEUR <- bitstampEUR[bitstampEUR$Time %nin% setdiff(bitstampEUR$Time, bitstampUSD$Time),]
    bitstampEUR <- bitstampEUR[bitstampEUR$Time %nin% setdiff(bitstampEUR$Time, truefxEURUSD$Time),]
    
    truefxEURUSD <- truefxEURUSD[truefxEURUSD$Time %nin% setdiff(truefxEURUSD$Time, bitstampUSD$Time),]
    truefxEURUSD <- truefxEURUSD[truefxEURUSD$Time %nin% setdiff(truefxEURUSD$Time, bitstampEUR$Time),]
    cat("Anzahl gemeinsamer Datensätze:", nrow(bitstampEUR), "\n")
    
    # Berechne Näherung für Bid/Ask
    bitstampEUR$CloseBid <- bitstampEUR$Close * (1 - spread_estimate_btceur / 100 / 2)
    bitstampEUR$CloseAsk <- bitstampEUR$Close * (1 + spread_estimate_btceur / 100 / 2)
    
    bitstampUSD$CloseBid <- bitstampUSD$Close * (1 - spread_estimate_btcusd / 100 / 2)
    bitstampUSD$CloseAsk <- bitstampUSD$Close * (1 + spread_estimate_btcusd / 100 / 2)
    
    # Berechne Cross Rates und vergleiche mit TrueFX-Kurs
    # Aktuell keine Bid/Ask-Daten für Bitcoin verfügbar
    # Nutze Geld- und Briefkurs von TrueFX
    cat("Berechne Daten für 60s-Schlusskurs\n")
    result_close <- data.table(
        Time = bitstampEUR$Time,
        
        # Verkauf BTC gegen EUR (Kauf EUR gegen BTC)
        bitstampEUR_Bid = bitstampEUR$CloseBid,
        # Kauf BTC gegen EUR (Verkauf EUR gegen BTC)
        bitstampEUR_Ask = bitstampEUR$CloseAsk,
        
        # Verkauf BTC gegen USD (Kauf USD gegen BTC)
        bitstampUSD_Bid = bitstampUSD$CloseBid,
        # Kauf BTC gegen USD (Verkauf USD gegen BTC)
        bitstampUSD_Ask = bitstampUSD$CloseAsk,
        
        # Verkauf EUR gegen USD (Kauf USD gegen EUR)
        truefxEURUSD_Bid = truefxEURUSD$CloseBid,
        # Kauf EUR gegen USD (Verkauf USD gegen EUR)
        truefxEURUSD_Ask = truefxEURUSD$CloseAsk
    )
    
    # Kauf BTC gegen EUR, Kauf USD gegen BTC, Kauf EUR gegen USD
    # EUR -> BTC -> USD -> EUR
    # Ergebnis in Prozent
    result_close$EUR_BTC_USD_EUR <- 
        (result_close$bitstampUSD_Bid / result_close$bitstampEUR_Ask / result_close$truefxEURUSD_Ask) - 1
    
    # Kauf BTC gegen USD, Kauf EUR gegen BTC, Kauf USD gegen EUR
    # USD -> BTC -> EUR -> USD
    result_close$USD_BTC_EUR_USD <- 
        (result_close$bitstampEUR_Bid / result_close$bitstampUSD_Ask * result_close$truefxEURUSD_Bid) - 1
    
    
    plot <- 
        result_close %>%
        
        # Boxplot des Volumens
        ggplot(aes(x = EUR_BTC_USD_EUR + 1)) +
        geom_density() +
        
        # "Fill"-Farben und Theme
        theme_minimal() +
        scale_color_pander() +
        
        labs(
            title="Verteilungsdichte EUR-Route",
            subtitle=paste0(
                "Quelle: bitcoincharts.com, Zeitraum: ", 
                format(min(result_close$Time), "%d.%m.%Y"),
                " bis ", format(max(result_close$Time), "%d.%m.%Y")
            ),
            x="1/BTCEUR(Ask) * BTCUSD(Bid) * 1/EURUSD(Ask)",
            y="P(x)"
        )
    
    print(plot)
    
    plot <- 
        result_close %>%
        
        # Boxplot des Volumens
        ggplot(aes(x = USD_BTC_EUR_USD + 1)) +
        geom_density() +
        
        # "Fill"-Farben und Theme
        theme_minimal() +
        scale_color_pander() +
        
        labs(
            title="Verteilungsdichte USD-Route",
            subtitle=paste0(
                "Quelle: bitcoincharts.com, Zeitraum: ", 
                format(min(result_close$Time), "%d.%m.%Y"),
                " bis ", format(max(result_close$Time), "%d.%m.%Y")
            ),
            x="1/BTCUSD(Ask) * BTCEUR(Bid) * EURUSD(Bid)",
            y="P(x)"
        )
    
    print(plot)
    
    # Reduziere auf signifikante Abweichungen
    signif_limit <- 0.01
    cat("Reduziere auf signifikante Abweichungen (>", signif_limit, "%)\n")
    result_close_signif <- result_close[
        result_close$EUR_BTC_USD_EUR > signif_limit | 
        result_close$USD_BTC_EUR_USD > signif_limit,
    ]
    
    cat("Anzahl signifikanter Datensätze:", nrow(result_close_signif), "\n")
    
    result_close <<- result_close
    result_close_signif <<- result_close_signif
    
})()

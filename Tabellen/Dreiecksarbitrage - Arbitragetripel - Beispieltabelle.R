library("fst")
library("data.table")
library("stringr") # str_replace
source("Dreiecksarbitrage/Auswertung gruppiert.R") # calculateResult
source("Funktionen/printf.R")
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")

# Dieses Beispiel entspricht der Route EUR -> BTC -> USD -> EUR
#                                 bzw. USD -> EUR -> BTC -> USD
arbitrageOpportunity <- as.POSIXct("2021-12-05 23:06:02.170219")

# Beispielzeilen
exampleTimeframe <- c("2021-12-05 23:06:01.086400", "2021-12-05 23:06:02.17022")

# 31400114 rows, 10 columns (coinbase-usd-eur.fst)
# Position 29.900.000 entspricht dem 03.12.2021, 20:52:48
# Position 30.000.000 entspricht dem 06.12.2021, 21:56:10
fst_from <- 29900000L
fst_to <- fst_from + 1e5

dataset <- read_fst(
    path = "Cache/Dreiecksarbitrage/1s/coinbase-usd-eur.fst",
    columns = c("Time", "a_PriceLow", "a_PriceHigh", "b_PriceLow", "b_PriceHigh", "ab_Bid", "ab_Ask"),
    from = fst_from,
    to = fst_to,
    as.data.table = TRUE
)

# Beispieltabelle bilden
dataset <- dataset[Time %between% exampleTimeframe]
printf("Tabelle OHNE Ergebnisse:\n\n")

# Spalten: Nr., Zeit, BTC/USD High/Low, BTC/EUR High/Low, EUR/USD Geld/Brief
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(dataset))) {
    priceTriple <- dataset[i]
    
    printf("%s%d &\n", tabIndentFirst, i)
    
    printf(
        "%s%s &\n",
        tabIndent, formatPOSIXctWithFractionalSeconds(priceTriple$Time, "%H:%M:%OS")
    )
    
    # BTC/USD
    printf("%s%s &\n", tabIndent, format.money(priceTriple$a_PriceHigh, digits=2))
    printf("%s%s &\n", tabIndent, format.money(priceTriple$a_PriceLow, digits=2))
    
    # BTC/EUR
    printf("%s%s &\n", tabIndent, format.money(priceTriple$b_PriceHigh, digits=2))
    printf("%s%s &\n", tabIndent, format.money(priceTriple$b_PriceLow, digits=2))
    
    # EUR/USD
    printf("%s%s &\n", tabIndent, format.money(priceTriple$ab_Bid, digits=5))
    printf("%s%s \\\\\n", tabIndent, format.money(priceTriple$ab_Ask, digits=5))
    
    printf("\n")
}


# Tabelle MIT Ergebnissen
result <- new(
    "TriangularResult",
    Exchange = "coinbase",
    ExchangeName = "Coinbase Pro",
    Currency_A = "usd",
    Currency_B = "eur",
    data = dataset
)
calculateResult(result)

# Spalten: Nr., Zeit, BTC/USD High/Low, BTC/EUR High/Low, Route 1, Route 2
printf("\n\nTabelle MIT Ergebnissen:\n\n")
for (i in seq_len(nrow(result$data))) {
    priceTriple <- result$data[i]
    
    printf("%s%d &\n", tabIndentFirst, i)
    
    printf(
        "%s%s &\n",
        tabIndent, formatPOSIXctWithFractionalSeconds(priceTriple$Time, "%H:%M:%OS")
    )
    
    # BTC/USD
    printf("%s%s &\n", tabIndent, format.money(priceTriple$a_PriceHigh, digits=2))
    printf("%s%s &\n", tabIndent, format.money(priceTriple$a_PriceLow, digits=2))
    
    # BTC/EUR
    printf("%s%s &\n", tabIndent, format.money(priceTriple$b_PriceHigh, digits=2))
    printf("%s%s &\n", tabIndent, format.money(priceTriple$b_PriceLow, digits=2))
    
    # Ergebnisse
    route_1 <- sprintf("%+.6f", (priceTriple$ResultAB) * 100) |> 
        str_replace(fixed("."), ",") |>
        str_replace("(\\d{3})", "\\1\\\\,")
    printf("%s%s\\,\\%% &\n", tabIndent, route_1)
    
    route_2 <- sprintf("%+.6f", (priceTriple$ResultBA) * 100) |> 
        str_replace(fixed("."), ",") |>
        str_replace("(\\d{3})", "\\1\\\\,")
    printf("%s%s\\,\\%% \\\\\n", tabIndent, route_2)
    
    printf("\n")
}

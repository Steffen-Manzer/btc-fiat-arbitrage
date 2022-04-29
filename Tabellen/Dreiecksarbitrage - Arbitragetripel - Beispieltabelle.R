library("fst")
library("data.table")
source("Funktionen/printf.R")
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")

#### TODO Copy&Paste aus Kolloquium-Skript, anpassen

# Dieses Beispiel entspricht der Route EUR -> BTC -> USD -> EUR
#                                 bzw. USD -> EUR -> BTC -> USD
arbitrageOpportunity <- as.POSIXct("2021-12-05 22:30:03.355915")

# Beispielzeilen
exampleTimeframe <- as.POSIXct(c("2021-12-05 22:30:03.355", "2021-12-05 22:30:05"))

# 39199305 rows, 10 columns (coinbase-usd-eur-1.fst)
# Position 37.600.000 entspricht dem 03.12.2021, 17:19
# Position 37.700.000 entspricht dem 06.12.2021, 15:28
fst_from <- 37600000L
fst_to <- fst_from + 1e5

data_total <- read_fst(
    path = "Cache/Dreiecksarbitrage/5s/coinbase-usd-eur-1.fst",
    from = fst_from,
    to = fst_to,
    as.data.table = TRUE
)

data_reduced <- data_total[Time %between% exampleTimeframe]

data_example <- data_total[Time == arbitrageOpportunity]

# Route: [ USD -> ] EUR -> BTC -> USD [ -> EUR ]
stopifnot(data_example$firstTick == "a")
printf("   [Kauf BTC gegen USD] 1 BTC = %s USD\n", format.money(data_example$a_PriceHigh))
printf("[Verkauf BTC gegen EUR] 1 BTC = %s EUR\n", format.money(data_example$b_PriceLow))
printf("   [Kauf USD gegen EUR] 1 EUR = %s USD\n", format.money(data_example$ab_Bid, digits = 5L))

printf(
    "Arbitragegewinn: 1 USD => %s USD\n",
    round(data_example$b_PriceLow / data_example$a_PriceHigh * data_example$ab_Bid, 9L)
)

printf("\n\nBeispieltabelle:\n")
# Als LaTeX-Tabelle ausgeben
# Spalten: Zeit, BTC/USD Low/High, BTC/EUR Low/High, EUR/USD Bid/Ask
tabIndentFirst <- strrep(" ", 8)
tabIndent <- strrep(" ", 12)
for (i in seq_len(nrow(data_reduced))) {
    priceTriple <- data_reduced[i]
    printf(
        "%s%s &\n",
        tabIndentFirst,
        formatPOSIXctWithFractionalSeconds(priceTriple$Time, "%d.%m.%Y, %H:%M:%OS")
    )
    
    #### In diesem kurzen Beispiel sind Höchst- und Tiefstkurse identisch.
    #### Zeige daher nur einen aus Gründen der Übersichtlichkeit
    # BTC/USD
    printf("%s%s\\,USD &\n", tabIndent, format.money(priceTriple$a_PriceLow, digits=2))
    #printf("%s%s &\n", tabIndent, format.money(priceTriple$a_PriceHigh, digits=2))
    
    # BTC/EUR
    printf("%s%s\\,EUR &\n", tabIndent, format.money(priceTriple$b_PriceLow, digits=2))
    #printf("%s%s &\n", tabIndent, format.money(priceTriple$b_PriceHigh, digits=2))
    
    # EUR/USD
    printf("%s%s\\,USD &\n", tabIndent, format.money(priceTriple$ab_Bid, digits=5))
    printf("%s%s\\,USD \\\\\n", tabIndent, format.money(priceTriple$ab_Ask, digits=5))
    
    printf("\n")
}

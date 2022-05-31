library("fst")
library("data.table")
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

data_total <- read_fst(
    path = "Cache/Dreiecksarbitrage/1s/coinbase-usd-eur.fst",
    columns = c("Time", "a_PriceLow", "a_PriceHigh", "b_PriceLow", "b_PriceHigh", "ab_Bid", "ab_Ask"),
    from = fst_from,
    to = fst_to,
    as.data.table = TRUE
)

# Spaltennamen für Beispielberechnung weit intuitiver gestalten
# coinbase-usd-eur-1 -> a = usd, b = eur, ab = eurusd (gemäß Konvention)
old <- c("a_PriceLow",      "a_PriceHigh",      "b_PriceLow",      "b_PriceHigh",      "ab_Bid",     "ab_Ask")
new <- c("btcusd_PriceLow", "btcusd_PriceHigh", "btceur_PriceLow", "btceur_PriceHigh", "eurusd_Bid", "eurusd_Ask")
setnames(data_total, old, new)

# Beispieltabelle
data_reduced <- data_total[Time %between% exampleTimeframe]

# Berechnungsbeispiel
data_example <- data_total[Time == arbitrageOpportunity]
rm(data_total)

# Route: EUR -> BTC -> USD -> EUR
#        USD -> EUR -> BTC -> USD
printf("Route 1: EUR -> BTC -> USD -> EUR\n")
printf("         USD -> EUR -> BTC -> USD\n")
printf("Beispielrechnung ausgehend von 1 Mio. EUR:\n")

# 1. EUR -> BTC
result <- round(1e6 / data_example$btceur_PriceLow, 8L)
printf(
    "   [Kauf BTC gegen EUR] 1 BTC = %s EUR => %.08f BTC\n",
    format.money(data_example$btceur_PriceLow), result
)

# 2. BTC -> USD
result <- round(result * data_example$btcusd_PriceHigh, 4L)
printf(
    "[Verkauf BTC gegen USD] 1 BTC = %s USD => %s USD\n",
    format.money(data_example$btcusd_PriceHigh), format.money(result)
)

# 3. USD -> EUR
result <- round(result / data_example$eurusd_Ask, 4L)
printf(
    "[Verkauf USD gegen EUR] 1 EUR = %s USD => %s EUR\n",
    format.money(data_example$eurusd_Ask, digits = 5L), format.money(result)
)

printf(
    "Arbitragegewinn (mit Zwischenrundung): 1.000.000 EUR => %s EUR (%+07f %%)\n\n",
    format.money(result), result/1e4 - 100
)

# Route: EUR -> USD -> BTC -> EUR
#        USD -> BTC -> EUR -> USD
# Hier noch ohne Zwischenrundung, aber diese Route wird in der Arbeit ohnehin nicht dargestellt
printf("Route 2: EUR -> USD -> BTC -> EUR\n")
printf("         USD -> BTC -> EUR -> USD\n")

# 1. USD -> BTC
result <- round(1e6 / data_example$btcusd_PriceLow, 8L)
printf("   [Kauf BTC gegen USD] 1 BTC = %s USD => %.08f BTC\n", data_example$btcusd_PriceLow, result)

# 2. BTC -> EUR
result <- round(result * data_example$btceur_PriceHigh, 4L)
printf("[Verkauf BTC gegen EUR] 1 BTC = %s EUR => %s EUR\n", data_example$btceur_PriceHigh,format.money(result))

# 3. EUR -> USD
result <- round(result * data_example$eurusd_Bid, 4L)
printf("   [Kauf USD gegen EUR] 1 EUR = %s USD => %s USD\n", data_example$eurusd_Bid, format.money(result))

printf(
    "Arbitragegewinn (mit Zwischenrundung): 1.000.000 USD => %s USD (%+.2f USD / %+07f %%)\n\n",
    format.money(result), result-1e6, result/1e4 - 100
)

library("data.table")
library("stringr")
source("Funktionen/printf.R")
source("Funktionen/FormatNumber.R")

data <- fread(
    "Daten/bitinfocharts/Vermoegensverteilung-2022-07-15.csv",
    select = c("Balance, BTC", "Addresses", "Coins")
)

# "Coins" einlesen: Suffix " BTC" entfernen, Dezimal-Kommas entfernen, als Zahl speichern
data[, Coins := Coins |> str_sub(1, -5) |> str_replace_all(fixed(","), "") |> as.numeric()]

# Bereiche unter 0,1 BTC zusammenfassen
data <- rbindlist(list(
    data[1:5, .("Balance, BTC"="[0 - 0.1)",Addresses=sum(Addresses),Coins=sum(Coins))],
    data[6:.N]
))

sumAddresses <- sum(data$Addresses)
sumCoins <- sum(data$Coins)

tabIndent <- strrep(" ", 12)
aggregatedAddresses <- 0
cumulatedAddresses <- sumAddresses
cumulatedCoins <- sumCoins
for (balanceIndex in seq_len(nrow(data))) {
    
    # Kontostand | Anzahl Adressen (n|%|Kum) | Anzahl Bitcoin (n|%|Kum)
    balanceData <- data[balanceIndex]
    
    balance <- balanceData$`Balance, BTC` |>
        str_replace(fixed("."), "_KOMMA_") |>
        str_replace_all(fixed(","), ".") |>
        str_replace(fixed("_KOMMA_"), ",") |>
        str_replace(fixed("[0 - 0,1)"), "$<$ 0,1") |>
        str_replace(fixed("[100.000 - 1.000.000)"), "$\\geq$ 100.000")
    
    percentageAddresses <- balanceData$Addresses / sumAddresses
    percentageAddressesCumulated <- cumulatedAddresses / sumAddresses
    cumulatedAddresses <- cumulatedAddresses - balanceData$Addresses
    
    percentageCoins <- balanceData$Coins / sumCoins
    percentageCoinsCumulated <- cumulatedCoins / sumCoins
    cumulatedCoins <- cumulatedCoins - balanceData$Coins
    
    # Formatierung
    if (round(percentageAddresses, 4L) >= 0.0001) {
        percentageAddressesFormatted <- format.percentage(percentageAddresses, 2L)
    } else {
        percentageAddressesFormatted <- "$<$ 0,01"
    }
    if (round(percentageAddressesCumulated, 4L) >= 0.0001) {
        percentageAddressesCumulatedFormatted <- format.percentage(percentageAddressesCumulated, 2L)
    } else {
        percentageAddressesCumulatedFormatted <- "$<$ 0,0"
    }
    
    cat(
        "        ", balance, " &\n",
        tabIndent, format.number(balanceData$Addresses), " &\n",
        tabIndent, percentageAddressesFormatted, "\\,\\% &\n",
        tabIndent, percentageAddressesCumulatedFormatted, "\\,\\% &\n",
        tabIndent, format.number(round(balanceData$Coins, 0)), " &\n",
        tabIndent, format.percentage(percentageCoins, 2L), "\\,\\% &\n",
        tabIndent, format.percentage(percentageCoinsCumulated, 2L), "\\,\\% \\\\\n\n",
        sep = ""
    )
    
}

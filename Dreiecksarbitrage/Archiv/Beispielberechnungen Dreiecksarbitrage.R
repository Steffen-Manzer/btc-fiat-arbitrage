
# Anmerkung: https://web.archive.org/web/20190708222753/http://www.fxmarkets.de:80/diploma/cross.htm
# 
# Ist die Quotierung(z.B. EUR-USD und USD-JPY)  "überkreuz", brauche ich die Kurse nicht "kreuzen"
# Geld = Geld * Geld
# Brief = Brief * Brief
#
# Ist die Quotierung der Währungen nicht "überkreuz" (z.B. bei EUR-USD und GBP-USD), muß ich die Kurse "kreuzen"
# Erinnerung: EURUSD_Geld = 1/USDEUR_Brief
#
# Vehikelwährung ist quotierte Währung: Beispiel USD für USDJPY und USDCHF:
# CHFJPY_Geld = USDJPY_Geld / USDCHF_Brief
# CHFJPY_Brief = USDJPY_Brief / USDCHF_Geld
#
# Vehikelwährung ist Gegenwährung: Beispiel USD für EURUSD und AUDUSD:
# EURAUD_Geld = EURUSD_Geld / AUDUSD_Brief
# EURAUD_Brief = EURUSD_Brief / AUDUSD_Geld


# Geltende Kurse
EURUSD.bid <- 1.1291 # Geldkurs
EURUSD.ask <- 1.1295 # Briefkurs

USDJPY.bid <- 108.60 # Geldkurs
USDJPY.ask <- 108.65 # Briefkurs

EURJPY.bid <- 122.50 # Geldkurs. Dieser Kurs ist "falsch"
EURJPY.ask <- 122.60 # Briefkurs. Dieser Kurs ist "falsch"

# Startwerte
start_EUR_USD_JPY_EUR <- 1
start_EUR_JPY_USD_EUR <- 1

# Anzuzeigende Dezimalstellen
numDigits <- 8

# Hilfsfunktion
showCurrency <- function(x) formatC(x, format="f", big.mark=".", decimal.mark=",", digits=numDigits)

# Berechnung
cat("Route EUR -> USD -> JPY -> EUR:\n")

result_usd <- round(start_EUR_USD_JPY_EUR * EURUSD.bid, numDigits)
cat("1 EUR =", showCurrency(result_usd), "USD\n")

result_jpy <- round(result_usd * USDJPY.bid, numDigits)
cat(showCurrency(result_usd), "USD *", showCurrency(USDJPY.bid), "=", showCurrency(result_jpy), "JPY\n")

JPYEUR.bid <- 1/EURJPY.ask
result_eur <- round(result_jpy * JPYEUR.bid, numDigits)
cat(showCurrency(result_jpy), "JPY /", showCurrency(EURJPY.ask), "=", showCurrency(result_eur), "EUR\n")

cat("Rohgewinn / -verlust:", showCurrency(result_eur - start_EUR_USD_JPY_EUR), "EUR\n")


# Berechnung 2
cat("\nRoute EUR -> JPY -> USD -> EUR:\n")

result_jpy <- round(start_EUR_JPY_USD_EUR * EURJPY.bid, numDigits)
cat("1 EUR =", showCurrency(result_jpy), "JPY\n")

JPYUSD.bid <- 1/USDJPY.ask
result_usd <- round(result_jpy * JPYUSD.bid, numDigits)
cat(showCurrency(result_jpy), "JPY /", showCurrency(USDJPY.ask), "=", showCurrency(result_usd), "USD\n")

USDEUR.bid <- 1/EURUSD.ask
result_eur <- round(result_usd * USDEUR.bid, numDigits)
cat(showCurrency(result_usd), "USD /", showCurrency(EURUSD.ask), "=", showCurrency(result_eur), "EUR\n")

cat("Rohgewinn / -verlust:", showCurrency(result_eur - start_EUR_JPY_USD_EUR), "EUR\n")


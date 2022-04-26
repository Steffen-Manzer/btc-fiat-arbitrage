# Arbitrage zwischen Bitcoin- und Devisenmärkten

Crawler, Aggregatoren und Algorithmen zur Analyse von Arbitragemöglichkeiten zwischen
Bitcoin- und Devisenmärkten.

![Raumarbitrage BTC/USD](https://research.noecho.de/Logo@2x.png)

## Versionshinweise

- Die Auswertungsskripte wurden unter R 4.1.3 entwickelt.
- Die in PHP geschriebenen Daten-Crawler wurden unter PHP 8.1 entwickelt.
- Die in Python geschriebenen Daten-Crawler wurden unter Python 3.9 entwickelt.

Ältere Versionen können mit den vorliegenden Skripten ebenfalls funktionieren, wurden 
jedoch nicht getestet.

## Verbindung zu LaTeX
Für eine automatische Datenaktualisierung aus LaTeX heraus wird in einigen Skripten der
LaTeX-Befehl `executeR` erwähnt:

```latex
% R aus LaTeX heraus ausführen
% Anmerkung: Der Dateiname sollte unter keinen Umständen irgendwelche
% Sonderzeichen (nicht-ASCII) enthalten. Leerzeichen sind in Ordnung.
\newcommand{\executeR}[1]{%
    \ignorespaces%
    \immediate%
    \write18{%
        echo;%
        echo "executeR: #1:";%
        cd "/Pfad/zu/den/R/Quelldateien";%
        R --no-save --no-restore --slave -f '#1' --args FromLaTeX;%
        echo;%
    }%
    \ignorespaces%
}
```

Bei Kompilierung der LaTeX-Dokumente (mit der Option -shell-escape) wird das angegebene
R-Skript ausgeführt. Diese erstellen bzw. aktualisieren Statistiken in Textform,
Abbildungen oder Tabellen, die in LaTeX eingebunden sind.


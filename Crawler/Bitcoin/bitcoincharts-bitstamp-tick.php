<?php

// Geschrieben für PHP 8.1 (CLI oder HTTP)

require_once '_include.php';

$src = strtolower($_GET['src'] ?? $argv[1] ?? '');
if ($src !== 'eur' && $src !== 'usd') {
    echo 'Invalid source.';
    exit(1);
}

// API-Quelle
// https://bitcoincharts.com/about/markets-api/
// Trade data is available as CSV, delayed by approx. 15 minutes. It will return the 2000 most recent trades.
define('API_URL', 'http://api.bitcoincharts.com/v1/trades.csv?symbol=bitstamp' . strtoupper($src));

// CSV-Ziel
define('CSV_BASE', DATA_DIR . "bitcoincharts-bitstamp/btc${src}/bitcoincharts-bitstamp-tick-btc${src}-");

// Status-File: Enthält letzte Datensätze für den nahtlosen Abgleich
define('STATE_FILE', DATA_DIR . "bitcoincharts-bitstamp/btc${src}/.state");

echo "Processing bitcoincharts btc${src}..." . PHP_EOL;

if (!file_exists(STATE_FILE)) {
    
    // Noch keine Daten gesammelt. Datei erzeugen und neu beginnen.
    echo "Starting new.\n";
    $lastDatasets = [];
    
} else {

    // Datensatz existiert, fortsetzen
    $lastDatasets = file(STATE_FILE);
    
    echo 'Last modified: ' . strftime('%Y-%m-%d %H:%M:%S', filemtime(STATE_FILE)) . PHP_EOL;
    echo 'Reading state from file: ' . STATE_FILE . ':' . PHP_EOL;
    echo 'Last dataset: ' . end($lastDatasets) . PHP_EOL;
    
    $lastDataset = explode(",", end($lastDatasets));
    $lastDatasetTime = readISODate($lastDataset[0]);
    
    // Zuletzt erfasster Datensatz ist weniger als eine Minute alt: Verhindere zu häufige Abfragen
    if ($lastDatasetTime > ( (new DateTime())->sub(new \DateInterval('PT1M')))) {
        echo 'Last dataset is too recent. Stop.';
        exit(1);
    }
}

// API abfragen
echo 'Querying ' . API_URL . PHP_EOL;

$data = file(API_URL);
if (empty($data)) {
    infoLog('Decoding data failed.');
    notifyAboutCriticalError('Empty response from server');
    echo 'Could not read response.';
    exit(1);
}
echo 'Received '. count($data) . ' datasets.' . PHP_EOL . PHP_EOL;

// Reihenfolge umkehren, da neueste zuerst erscheinen
$data = array_reverse($data);


// Ergebnis zusammenstellen
$newDatasets = 0;
$hasDupes = false;
$result = [];
$last20Results = [];
foreach ($data as $line) {
    
    $tick = str_getcsv($line);
    $tick = array_combine(['Time', 'Price', 'Amount'], $tick);
    
    // Datum formatieren: Unix-Timestamp
    $time = \DateTime::createFromFormat('U', $tick['Time']);
    
    // Sicherheitsnetz: Auf gültigen Zeitraum prüfen
    if (
        $time === false ||
        (int)$time->format('Y') < 2010 ||
        (int)$time->format('Y') > 2030
    ) {
        infoLog('Parsed date outside valid range (2010-2030)!');
        notifyAboutCriticalError('Parsed date outside valid range (2010-2030)');
        echo 'Parsed: ' . getISODate($time) . PHP_EOL;
        var_dump($line);
        echo 'Result set:' . PHP_EOL;
        var_dump($json);
        exit(1);
    }
    
    // Datensatz aufbereiten: Zeitstempel, Preis, Volumen
    $tickLine = sprintf(
        '%s,%s,%s',
        getISODate($time),
        $tick['Price'],
        $tick['Amount']
    );
    
    // Datensatz bereits erfasst
    if (isset($lastDatasetTime) && $time < $lastDatasetTime) {
        $hasDupes = true;
        echo "Dupe: ${tickLine}\n";
        continue;
    }
    
    // Selbe Sekunde wie letzter Datensatz, eventuell bereits erfasst: Weitere Prüfung
    if (isset($lastDatasetTime) && $time == $lastDatasetTime) {
        
        // Prüfe alle vorhandenen Datensätze in dieser Sekunde
        foreach ($lastDatasets as $existingData) {
            if ($tickLine == trim($existingData)) {
                $hasDupes = true;
                echo "Dupe: ${tickLine}\n";
                continue 2;
            }
        }
        
    }
    
    
    if (!isset($result[$time->format("Y-m")])) {
        $result[$time->format("Y-m")] = "";
    }
    
    // Datensatz hinzufügen
    echo "Tick: ${tickLine}\n";
    
    $result[$time->format("Y-m")] .= $tickLine . PHP_EOL;
    $newDatasets++;
    $last20Results[] = $tickLine;
}
$last20Results = array_slice($last20Results, -200);

// Keine neuen Datensätze: Ende
if (empty($result)) {
    echo 'No new datasets.';
    exit(1);
}

if (!$hasDupes) {
    notifyAboutCriticalError('No duplicates found');
    echo 'No duplicates found?';
    exit(1);
}

// Ergebnis in Datei speichern
// Ergebnis in Datei speichern
foreach ($result as $month => $ticks) {
    $csvFile = CSV_BASE . $month . ".csv.gz";
    
    // Monatsdatei noch nicht vorhanden
    if (!file_exists($csvFile)) {
        file_put_contents($csvFile, gzencode('Time,Price,Amount' . PHP_EOL));
    }
    
    $ticks = gzencode($ticks);
    file_put_contents($csvFile, $ticks, FILE_APPEND);
}

infoLog('BTC' . strtoupper($src) . ': Collected ' . number_format($newDatasets, 0, ',', '.') . ' new datasets.');

file_put_contents(STATE_FILE, implode(PHP_EOL, $last20Results));

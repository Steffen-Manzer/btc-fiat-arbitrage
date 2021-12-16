<?php

// Geschrieben für PHP 8.1 (CLI oder HTTP)

require_once '_include.php';

$src = strtolower($_GET['src'] ?? $argv[1] ?? '');
if ($src !== 'eur' && $src !== 'usd' && $src !== 'gbp') {
    echo 'Invalid source.';
    exit(1);
}

// API-Quelle
define('API_URL', 'https://api.pro.coinbase.com/products/BTC-' . strtoupper($src) . '/trades');

// CSV-Ziel
define('CSV_BASE', DATA_DIR . "coinbase/btc${src}/coinbase-tick-btc${src}-");

// Status-File: Enthält zuletzt abgefragten Datensatz für lückenlose Abfrage
define('LAST_DATASET', DATA_DIR . "coinbase/btc${src}/.lastDataset");


// Ab welcher ID soll das Crawling beginnen, wenn noch keine Daten vorliegen?
define('INITIAL_START_PAGE_USD', 31503603); // 2018-01-01T00:00:01.115Z = Erster Datensatz 2018
define('INITIAL_START_PAGE_EUR', 8814490); // 2018-01-01T00:00:06.608Z = Erster Datensatz 2018
define('INITIAL_START_PAGE_GBP', 2544102); // 2018-01-01T00:00:35.107Z = Erster Datensatz 2018


if (!file_exists(LAST_DATASET)) {
    
    // Noch keine Daten gesammelt. Datei erzeugen und von vorne beginnen.
    echo 'Starting new.' . PHP_EOL;
    if ($src === 'usd') {
        $lastStoredID = INITIAL_START_PAGE_USD;
    } elseif ($src === 'eur') {
        $lastStoredID = INITIAL_START_PAGE_EUR;
    } elseif ($src === 'gbp') {
        $lastStoredID = INITIAL_START_PAGE_GBP;
    } else {
        die("Unknown start page.");
    }
    
} else {
    
    // Datensatz existiert, fortsetzen
    // Lese letzten Datensatz, um Duplikate zu vermeiden
    $lastDataset = explode(',', file_get_contents(LAST_DATASET));

    printLog('Last dataset in CSV:', $lastDataset[0], '@', $lastDataset[1], PHP_EOL);
    
    $lastDatasetTime = \DateTime::createFromFormat(TIMEFORMAT, $lastDataset[1]);
    // Letzter Datensatz zuletzt vor weniger als einer Minute eingelesen
    if ($lastDatasetTime > ( (new DateTime())->sub(new DateInterval('PT1M')))) {
        echo 'Last dataset is too recent. Stop.';
        exit(1);
    }
    
    $lastStoredID = (int)$lastDataset[0];
    unset($lastDataset);
}

printLog('Reading data for BTC/' . strtoupper($src));

// Coinbase braucht einen User Agent und die passenden Kopfzeilen. Simuliere macOS 10.15.2 mit Safari
$ctx = stream_context_create([
    'http' => [
        'header' =>
            "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15\r\n" . 
            "Accept-Language: de-de\r\n" .
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n" . 
            "Accept-Encoding: gzip, deflate, br\r\n" . 
            "Connection: Close\r\n"
    ]
]);

$data = [];

// API bis zu 24x in sechser-Bursts abfragen, um restlos alle neuen Daten zu laden
$numAPIRequests = 24;

printLog("Querying up to ${numAPIRequests} times.");

$startPage = $lastStoredID;
for ($i = 0; $i < $numAPIRequests; $i++) {
    $startPage += 100;
    $url = API_URL . '?' . http_build_query(['after' => $startPage]);
    
    printLog('Querying ' . $url);

    $json = file_get_contents($url, false, $ctx);
    if ($json === false) {
        echo "Waiting 5s for retry...\n";
        sleep(5);
        $json = file_get_contents($url, false, $ctx);
    }

    // Daten mit hoher Wahrscheinlichkeit GZip-komprimiert
    if (($thisData = gzdecode($json)) === false) {
        $thisData = $json;
    }

    $thisData = json_decode($thisData, true);
    if (empty($json) || !is_array($thisData)) {
        notifyAboutCriticalError('Could not decode response: ' . json_last_error_msg());
        echo 'Could not decode response: ' . json_last_error_msg() . PHP_EOL;
        echo 'Received data: ' . PHP_EOL;
        infoLog('Decoding data failed.');
        var_dump($json);
        exit(1);
    }

    // Neueste zuerst, daher muss Reihenfolge umgekehrt werden
    $thisData = array_reverse($thisData);
    $data = [...$data, ...$thisData];
    
    // Es sind keine weiteren Daten zu erwarten - keine weiteren Abfragen
    if (end($thisData)['trade_id'] !== $startPage - 1) {
        echo "Keine weitere Abfrage, Ende erreicht.\n";
        break;
    }
    
    // "We throttle public endpoints by IP: 3 requests per second, up to 6 requests per second in bursts."
    if (($i+1) % 6 === 0 && $i !== $numAPIRequests - 1) {
        echo "Sleeping 2s...\n";
        sleep(2);
    }
}

$data = array_unique($data, SORT_REGULAR);


printLog('Received', count($data), 'datasets', PHP_EOL);

if (empty($data)) {
    notifyAboutCriticalError('Received dataset is empty.');
    echo 'Received dataset is empty.';
    exit(1);
}

// Ergebnis zusammenstellen
$numNewDatasets = 0;
$result = [];
$dupeFound = false;
$lastDatasetInData = '';
echo '      ID       Time                        Volume     Price         is_sell' . PHP_EOL;
foreach ($data as $tick) {
    
    $tick = (object)$tick;
    
    // Datum einlesen
    // Format: 2020-01-07T13:16:38.55Z
    //    oder 2020-01-07T13:16:38Z
    $time = \DateTime::createFromFormat('Y-m-d\TH:i:s.u\Z', $tick->time);
    if ($time === false) {
        $time = \DateTime::createFromFormat('Y-m-d\TH:i:s\Z', $tick->time);
    }
    
    // Auf gültigen Zeitraum prüfen
    if (
        $time === false ||
        (int)$time->format('Y') < 2010 ||
        (int)$time->format('Y') > 2030
    ) {
        notifyAboutCriticalError('Parsed date outside valid range (2010-2030)');
        infoLog('Parsed date outside valid range (2010-2030)!');
        echo 'Parsed: ' . getISODate($time) . PHP_EOL;
        var_dump($tick);
        echo 'Result set:' . PHP_EOL;
        var_dump($json);
        exit(1);
    }
    
    $tick->time = getISODate($time);
    
    // Datensatz aufbereiten: ID, Zeitstempel, Volumen, Preis, Art
    $tickLine = sprintf(
        '%s,%s,%s,%s,%s',
        $tick->trade_id,
        $tick->time,
        $tick->size,
        $tick->price,
        (int)($tick->side === 'sell') // 0 =  buy / 1 = sell
    );
    
    // Dieser Datensatz ist bereits erfasst worden
    if ($tick->trade_id <= $lastStoredID) {
        $dupeFound = true;
        echo 'Dupe: ' . $tickLine . PHP_EOL;
        continue;
    }
    
    if (!isset($result[$time->format("Y-m")])) {
        $result[$time->format("Y-m")] = "";
    }
    
    // Datensatz hinzufügen
    echo 'Tick: ' . $tickLine . PHP_EOL;
    $result[$time->format("Y-m")] .= $tickLine . PHP_EOL;
    $lastDatasetInData = $tickLine;
    $numNewDatasets++;
}

// Keine neuen Datensätze: Ende
if (empty($result)) {
    echo 'No new datasets.';
    exit(1);
}

if (!$dupeFound) {
    notifyAboutCriticalError('No duplicates found');
    echo 'No duplicates found';
    exit(1);
}

// Ergebnis in Datei speichern
foreach ($result as $month => $ticks) {
    $csvFile = CSV_BASE . $month . ".csv.gz";
    
    // Monatsdatei noch nicht vorhanden
    if (!file_exists($csvFile)) {
        file_put_contents($csvFile, gzencode('ID,Time,Amount,Price,Type' . PHP_EOL));
    }
    
    $ticks = gzencode($ticks);
    file_put_contents($csvFile, $ticks, FILE_APPEND);
}

infoLog('BTC' . strtoupper($src) . ": Collected ${numNewDatasets} new datasets.");
file_put_contents(LAST_DATASET, $lastDatasetInData);

<?php

// Geschrieben für PHP 8.1 (CLI oder HTTP)

require_once '_include.php';

// API-Quelle
// https://production.api.coindesk.com/v2/price/values/BTC?start_date=2020-11-27T11:00&end_date=2020-11-27T13:13&ohlc=false
define('API_URL', 'https://production.api.coindesk.com/v2/price/values/BTC');

// CSV-Ziel
define('CSV_FILE', DATA_DIR . 'coindesk/coindesk-bpi-close-60s.csv.gz');

echo 'Processing 60s close...' . PHP_EOL;

if (!file_exists(CSV_FILE) || filesize(CSV_FILE) === 0) {
    
    // Noch keine Daten gesammelt. Datei erzeugen und von vorne beginnen.
    file_put_contents(CSV_FILE, gzencode('Time,Close') . PHP_EOL);
    $lastDataset = new \DateTime('2010-07-18');
    
} else {
    
    // Lese letzten Datensatz
    echo 'Reading last dataset from CSV: ' . CSV_FILE . PHP_EOL;
    
    $lastChunk = gzfile_get_last_chunk_of_concatenated_file(CSV_FILE);
    if (empty($lastChunk)) {
        notifyAboutCriticalError('Could not read last chunk from CSV');
        echo 'Could not read last chunk from CSV.';
        exit(1);
    }

    $chunkLines = explode(PHP_EOL, $lastChunk);
    $lastLine = array_pop($chunkLines);
    
    // last line is empty
    if ($lastLine === '') {
        $lastLine = array_pop($chunkLines);
    }
    
    // Speicher wieder freigeben
    unset($lastChunk, $chunkLines);
    
    $lastLine = str_getcsv($lastLine);
    
    try {
        $lastDataset = readISODate($lastLine[0]);
    } catch (Exception $e) {
        notifyAboutCriticalError('Could not parse last dataset');
        echo 'Could not parse last dataset: ' . $lastLine[0];
        exit(1);
    }
}

// CSV nicht beschreibbar
if (!is_writeable(CSV_FILE)) {
    notifyAboutCriticalError('CSV not writeable');
    echo 'Could not open target CSV file for writing.';
    exit(1);
}

// API abfragen
echo 'Last dataset:          ' . $lastDataset->format('Y-m-d H:i') . PHP_EOL;

$requestFrom = clone $lastDataset;
$requestFrom->sub(new \DateInterval('PT1M'));

$requestUntil = clone $lastDataset;
$requestUntil->add(new \DateInterval('PT12H'));

echo 'Querying ' . $requestFrom->format('Y-m-d H:i') . ' - ' . $requestUntil->format('Y-m-d H:i') . PHP_EOL;

if ($requestUntil >= (new DateTime())) {
    echo 'Next dataset is too recent. Stop.';
    exit(1);
}

$url = API_URL . '?' . str_replace('%3A', ':', http_build_query([
    'start_date' => $requestFrom->format('Y-m-d\TH:i'),
    'end_date' => $requestUntil->format('Y-m-d\TH:i'),
    'ohlc' => 'false'
]));

echo 'Querying ' . $url . PHP_EOL;
$result = file_get_contents($url);

$data = json_decode($result);
if ($data === null || (int)$data->statusCode !== 200 || !isset($data->data) || !is_array($data->data->entries)) {
    notifyAboutCriticalError('Could not decode response: ' . json_last_error_msg());
    echo 'Could not decode response: ' . json_last_error_msg() . PHP_EOL;
    echo 'Received data: ' . PHP_EOL;
    print_r($result);
    exit(1);
}

$data = $data->data->entries;

echo 'Received data: ' . strlen($result) . ' bytes / '. count($data) . ' datasets' . PHP_EOL . PHP_EOL;

if (empty($data)) {
    notifyAboutCriticalError('Received dataset is empty.');
    echo 'Received dataset is empty.';
    exit(1);
}

// Ergebnis zusammenstellen
$result = '';
foreach ($data as $tick) {
    
    $tick = array_combine(['Time', 'Close'], $tick);
    $time = \DateTime::createFromFormat('U.u', sprintf('%f', $tick['Time'] / 1000));
    
    // skip datasets out of range
    if ($time <= $lastDataset || $time >= $requestUntil) {
        echo '-- Skipping ' . $time->format('Y-m-d H:i') . PHP_EOL;
        continue;
    }
    
    echo 'Got tick: ' . $time->format('Y-m-d H:i') . ', ';
    echo 'Price: ' . asPrice($tick['Close']) . ' USD' . PHP_EOL;
    $tick['Time'] = getISODate($time);
    
    $result .= implode(',', array_values($tick)) . PHP_EOL;
}

// Keine neuen Datensätze
if (empty($result)) {
    echo 'No new datasets.';
    exit(1);
}

// Ergebnis in Datei speichern
$result = gzencode($result);
infoLog(
    'Collected ' . number_format(count($data), 0, ',', '.') . ' datasets. ' . 
    'Writing ' . round(strlen($result)/1024) . ' kB gzip to target file.'
);

file_put_contents(CSV_FILE, $result, FILE_APPEND);

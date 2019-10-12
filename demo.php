<?php

require __DIR__ . '/vendor/autoload.php';

use Hbase\Hbase;

try {
    $host  = 'localhost';

    $port  = 9090;

    $table = 'namespace:table';

    Hbase::init($host, $port);

    $rowKey = '1241255';

    Hbase::getInstance()->put($table, $rowKey, [['family' => 'cf', 'qualifier' => 'create_time', 'value' => '2019-04-27 18:01:12.9']]);

    $result = Hbase::getInstance()->get($table, $rowKey);
} catch (\Throwable $th) {
    echo 'query HBase with ' . $th->getMessage();
}

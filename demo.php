<?php

require __DIR__ . '/vendor/autoload.php';

use Hbase\Hbase;

try {
    $host  = 'localhost';

    $port  = 9090;

    $table = 'namespace:table';

    Hbase::init($host, $port, $table);

    $rowKey = '1241255';
    var_dump(Hbase::getInstance()->get($rowKey));
} catch (\Throwable $th) {
    echo 'query HBase with ' . $th->getMessage();
}

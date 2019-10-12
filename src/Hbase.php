<?php

namespace Hbase;

use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;

class Hbase
{
    protected $client;
    protected $table;

    public function __construct(string $host, int $port, string $table)
    {
        $socket    = new TSocket($host, $port);
        $transport = new TBufferedTransport($socket);
        $protocol  = new TBinaryProtocol($transport);

        $this->table  = $table;
        $this->client = new THBaseServiceClient($protocol);
    }

    public function get(string $rowKey)
    {
        return $this->client;
    }
}

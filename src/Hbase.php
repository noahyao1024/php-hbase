<?php

namespace Hbase;

use TColumnValue;
use TGet;
use THBaseServiceClient;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;
use TPut;

class Hbase
{
    protected static $instance;

    protected static $thriftHost;
    protected static $thriftPort;

    protected $client;
    protected $transport;

    /**
    * Method  getInstance
    *
    * @author Morysky
    * @static
    */
    public static function init(string $host, int $port)
    {
        static::$thriftHost = $host;
        static::$thriftPort = $port;
    }

    /**
     * Method  getInstance
     *
     * @author Morysky
     * @static
     * @return static
     */
    public static function getInstance()
    {
        if (static::$instance instanceof static) {
            return static::$instance;
        }

        return static::$instance = new static;
    }

    public function __construct()
    {
        $socket          = new TSocket(self::$thriftHost, self::$thriftPort);
        $this->transport = new TBufferedTransport($socket);
        $protocol        = new TBinaryProtocol($this->transport);
        $this->client    = new THBaseServiceClient($protocol);

        $this->transport->open();
    }

    public function __destruct()
    {
        $this->transport->close();
    }

    public function get(string $table, string $rowKey)
    {
        $tget      = new TGet();
        $tget->row = static::getRealRowKeys($rowKey);

        $response = $this->client->get($table, $tget);

        return array_map(function ($column) {
            return [
                'qualifier' => $column->qualifier,
                'value'     => $column->value,
                'timestamp' => $column->timestamp,
                /* Mask useless field
                   'family' => $column->family,
                */
            ];
        }, $response->columnValues);
    }

    public function put(string $table, string $rowKey, array $columns)
    {
        $tput      = new TPut();
        $tput->row = static::getRealRowKeys($rowKey);

        foreach ($columns as $column) {
            $tcolumnValue = new TColumnValue();

            $tcolumnValue->family    = $column['family']    ?? '';
            $tcolumnValue->qualifier = $column['qualifier'] ?? '';
            $tcolumnValue->value     = $column['value']     ?? '';

            $tput->columnValues[] = $tcolumnValue;
        }

        $this->client->put($table, $tput);
    }

    private static function getRealRowKeys(string $rowKey)
    {
        return substr(md5($rowKey), 24, 8) . '_' . $rowKey;
    }
}

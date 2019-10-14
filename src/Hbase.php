<?php

namespace Hbase;

use TColumnValue;
use TGet;
use THBaseServiceClient;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;
use TIOError;
use TPut;
use TResult;

/**
 * Class Hbase
 *
 * @package Hbase
 */
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

    /**
     * @param string $table
     * @param string $rowKey
     *
     * @throws TIOError
     * @return array
     */
    public function get(string $table, string $rowKey)
    {
        $tGet      = new TGet();
        $tGet->row = static::realRowkey($rowKey);

        $result = $this->client->get($table, $tGet);

        return static::toArray($result);
    }

    /**
     * @param string $table
     * @param array  $rowKeys
     *
     * @throws TIOError
     * @return array
     */
    public function getMultiple(string $table, array $rowKeys)
    {
        $tGets = [];

        foreach ($rowKeys as $rowKey) {
            $tGet      = new TGet();
            $tGet->row = static::realRowkey($rowKey);

            $tGets[] = $tGet;
        }

        $results = $this->client->getMultiple($table, $tGets);

        // Transfer it to plain array
        return array_map(function ($result) {
            return static::toArray($result);
        }, $results);
    }

    /**
     * @param string $table
     * @param string $rowKey
     * @param array  $columns
     *
     * @throws TIOError
     */
    public function put(string $table, string $rowKey, array $columns)
    {
        $tPut      = new tPut();
        $tPut->row = static::realRowkey($rowKey);

        foreach ($columns as $column) {
            $tcolumnValue = new TColumnValue();

            $tcolumnValue->family    = $column['family'] ?? '';
            $tcolumnValue->qualifier = $column['qualifier'] ?? '';
            $tcolumnValue->value     = $column['value'] ?? '';

            $tPut->columnValues[] = $tcolumnValue;
        }

        $this->client->put($table, $tPut);
    }

    private static function realRowkey(string $rowKey)
    {
        return substr(md5($rowKey), 24, 8) . '_' . $rowKey;
    }

    private static function toArray($tResult)
    {
        if (!$tResult instanceof TResult) {
            return [];
        }

        return array_map(function ($column) {
            return [
                'family'    => $column->family,
                'qualifier' => $column->qualifier,
                'value'     => $column->value,
                'timestamp' => $column->timestamp,
            ];
        }, $tResult->columnValues);
    }
}

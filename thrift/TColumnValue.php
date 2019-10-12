<?php
/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;
use Thrift\Type\TMessageType;
use Thrift\Exception\TException;
use Thrift\Exception\TProtocolException;
use Thrift\Protocol\TProtocol;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Exception\TApplicationException;

/**
 * Represents a single cell and its value.
 */
class TColumnValue extends TBase
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'family',
            'isRequired' => true,
            'type' => TType::STRING,
        ),
        2 => array(
            'var' => 'qualifier',
            'isRequired' => true,
            'type' => TType::STRING,
        ),
        3 => array(
            'var' => 'value',
            'isRequired' => true,
            'type' => TType::STRING,
        ),
        4 => array(
            'var' => 'timestamp',
            'isRequired' => false,
            'type' => TType::I64,
        ),
        5 => array(
            'var' => 'tags',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
    );

    /**
     * @var string
     */
    public $family = null;
    /**
     * @var string
     */
    public $qualifier = null;
    /**
     * @var string
     */
    public $value = null;
    /**
     * @var int
     */
    public $timestamp = null;
    /**
     * @var string
     */
    public $tags = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            parent::__construct(self::$_TSPEC, $vals);
        }
    }

    public function getName()
    {
        return 'TColumnValue';
    }


    public function read($input)
    {
        return $this->_read('TColumnValue', self::$_TSPEC, $input);
    }


    public function write($output)
    {
        return $this->_write('TColumnValue', self::$_TSPEC, $output);
    }

}

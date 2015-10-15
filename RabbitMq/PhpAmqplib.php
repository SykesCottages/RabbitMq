<?php

namespace Sykes\RabbitMq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * Wrapper class to manage requests to RabbitMq
 *
 * @class RabbitMq
 */
class PhpAmqplib
{
    /**
     * @type AMQPStreamConnection
     */
    private $connection;

    /**
     * @type AMQPChannel
     */
    private $channel;

    /**
     * Connect to RabbitMq based on environment
     */
    public function connect($host = '127.0.0.1', $port = 5672, $login = 'guest', $password = 'guest')
    {
        $this->connection = new AMQPStreamConnection(
            $host,
            $port,
            $login,
            $password
        );

        $this->channel = $this->connection->channel();
    }

    /**
     * Close the connection
     */
    public function disconnect()
    {
        $this->channel->close();
        $this->connection->close();
    }

    /**
     * Adds a message into a queue
     *
     * @param string $queue   Queue Name, cannot start with `amq.`
     * @param string $message Message to send
     */
    public function add_to_queue($queue, $message)
    {
        $this->connect();
        $this->channel->basic_publish(
            new AMQPMessage($message, ['delivery_mode' => 2]),
            $queue
        );
        $this->disconnect();
    }

    /**
     * Consume the queue and bind this to a callback, the callback should be wrapped in a try catch,
     * if an exception has been thrown then the message should be rejected into the dead letter
     * queue
     *
     * @param string                    $queue    Name of the queue, cannot start with `amq.`
     * @param string | array | callable $callback Callback function, either the name of the function if
     *                                            its in global scope, an array if its in object scope
     *                                            i.e. [ $this->feed_model, 'test' ], or an anonymous
     *                                            function
     */
    public function consume($queue, $callback)
    {
        $this->connect();
        $this->channel->basic_consume($queue, '', false, false, false, false, $callback);

        while (count($this->channel->callbacks)) {
            $this->channel->wait();
        }
        $this->disconnect();
    }

    /**
     * Read a queue, send an acknowledgement, useful for checking the status of dead letters. Used in testing
     *
     * @param $queue
     *
     * @return mixed
     */
    public function read_queue($queue)
    {
        $this->connect();
        $msg = $this->channel->basic_get($queue);

        if ($msg) {
            $this->channel->basic_ack($msg->delivery_info['delivery_tag']);
        }

        $this->disconnect();

        return $msg;
    }

    /**
     * Purge a queue, used in unit tests
     *
     * @param $queue
     */
    public function purge($queue)
    {
        $this->connect();
        $this->channel->queue_purge($queue);
        $this->disconnect();
    }
}

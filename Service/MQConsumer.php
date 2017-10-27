<?php
namespace ZJWorkerman\Service;

use Workerman\Worker;
use ZJPHP\Base\ZJPHP;
use ZJPHP\Base\Component;
use ZJPHP\Base\Event;
use ZJPHP\Service\Debugger;
use ZJPHP\Facade\ZJRedis;
use ZJPHP\Facade\Database;
use ZJPHP\Base\Exception\InvalidConfigException;
use ZJPHP\Base\Exception\InvalidCallException;
use ZJPHP\Base\Exception\InvalidParamException;
use ZJWorkerman\Base\Exception\MQException;
use Bunny\Channel;
use Bunny\Async\Client;
use Bunny\Message;
use Exception;
use Throwable;

class MQConsumer extends Component
{
    protected $rabbitmqOpts = [];
    protected $qos = 1;
    protected $msgPoolTtl = 2592000; // 30 days
    protected $exchange = '';
    protected $maxRetryTimes = 10;

    private $_client = null;

    public function start(Worker $worker, $queue, $routing_keys, $callback)
    {
        $event_loop = $worker->getEventLoop();
        $this->_client = new Client($event_loop, $this->rabbitmqOpts);

        $qos = $this->qos;
        if (empty($this->exchange) && empty($routing_keys)) {
            $this->onWorkQueue($qos, $queue, $callback);
        } elseif (!empty($this->exchange) && !empty($routing_keys)) {
            $this->onTopic($qos, $queue, $routing_keys, $callback);
        } else {
            throw new InvalidParamException('Unsupported Consumer Mode.');
        }
    }

    protected function onWorkQueue($qos, $queue, $callback)
    {
        $this->_client->connect()->then(function (Client $client) {
            return $client->channel();
        })->then(function (Channel $channel) use ($qos) {
            return $channel->qos(0, $qos)->then(function () use ($channel) {
                return $channel;
            });
        })->then(function (Channel $channel) use ($queue) {
            return $channel->queueDeclare($queue, false, true, false, false)->then(function () use ($channel) {
                return $channel;
            });
        })->then(function (Channel $channel) use ($callback, $queue) {
            echo ' [*] Waiting for messages from queue - ' . $queue, "\n";

            $channel->consume($callback, $queue);
        })->otherwise(function (Exception $e) {
            $err = new MQException($e->getMessage(), $e->getCode(), $e);
            throw $err;
        })->otherwise(function (Throwable $e) {
            $err = new MQException($e->getMessage(), $e->getCode(), $e);
            throw $err;
        })->done();
    }

    protected function onTopic($qos, $queue, $routing_keys, $callback)
    {
        $exchange = $this->exchange;

        $this->_client->connect()->then(function (Client $client) {
            return $client->channel();
        })->then(function (Channel $channel) use ($qos) {
            return $channel->qos(0, $qos)->then(function () use ($channel) {
                return $channel;
            });
        })->then(function (Channel $channel) use ($exchange, $queue, $routing_keys) {
            return $channel->exchangeDeclare(
                $exchange,
                'x-delayed-message',
                false, // passive
                true, // durable
                false, // auto_delete
                false, // internal
                false, // nowait
                ['x-delayed-type' => 'topic']
            )->then(
                function () use ($channel, $queue) {
                    $channel->queueDeclare(
                        $queue,
                        false, // passive
                        true, // durable
                        false, // exclusive
                        false, // auto_delete
                        false // no wait
                    );
                }
            )->then(
                function () use ($exchange, $channel, $queue, $routing_keys) {
                    foreach ($routing_keys as $routing_key) {
                        $channel->queueBind(
                            $queue,
                            $exchange,
                            $routing_key
                        );
                    }

                    return $channel;
                }
            );
        })->then(function (Channel $channel) use ($callback, $exchange, $queue) {
            echo ' [*] Waiting for messages from queue - ' . $queue . " binding to exchange - " .  $exchange, "\n";

            $channel->consume($callback, $queue);
        })->otherwise(function (Exception $e) {
            $err = new MQException($e->getMessage(), $e->getCode(), $e);
            throw $err;
        })->otherwise(function (Throwable $e) {
            $err = new MQException($e->getMessage(), $e->getCode(), $e);
            throw $err;
        })->done();
    }

    public function checkRetry($message_signature)
    {
        $redis_client = ZJRedis::connect();
        $key = "MQRetryCounter:" . $message_signature;
        $existed = $redis_client->exists($key);
        $retry_times = $redis_client->incr($key);
        if (!$existed) {
            $redis_client->expire($key, 1800); // Retry counter ttl 30 mins
        }
        $result = $retry_times <= $this->maxRetryTimes;

        if ($result === false) {
            $error_msg = 'Message retry times exceeds ' . $this->maxRetryTimes
                . ' on Application - ' . ZJPHP::$app->getAppName()
                . ' with message signature ' . $message_signature;
            trigger_error($error_msg, E_USER_WARNING);
        }

        return $result;
    }

    public function logMessage(Message $message)
    {
        $envelope = json_decode($message->content, true);

        $data = [
            'binding_key' => $message->routingKey,
            'message_content' => $envelope['content'],
            'signature' => $envelope['signature'],
            'created_at' => date('Y-m-d H:i:s')
        ];

        $data['id'] = Database::table('recieved_message_queue')->insertGetId($data);
        $data['alg'] = $envelope['alg'];

        return $data;
    }

    public function updateMessageLog($message_log_id, $ack_or_nack)
    {
        Database::table('recieved_message_queue')->where('id', $message_log_id)->update([
            'has_acked' => $ack_or_nack,
            'acked_at' => date('Y-m-d H:i:s')
        ]);
    }

    // Not for MQ but processor's transaction
    public function internalAck($signature)
    {
        $redis_client = ZJRedis::connect();
        $key = "MQConsumerPool:app-" . ZJPHP::$app->getAppName();
        $existed = $redis_client->exists($key);
        $result = $redis_client->sAdd($key, $signature);
        if (!$existed) {
            $redis_client->expire($key, $this->msgPoolTtl);
        }
        return $result;
    }

    // Local Internal
    public function isMsgDuplicate($signature)
    {
        $redis_client = ZJRedis::connect();
        $key = "MQConsumerPool:app-" . ZJPHP::$app->getAppName();

        return $redis_client->sIsMember($key, $signature);
    }

    public function stop()
    {
        if (!is_null($this->_client)) {
            $this->_client->disconnect();
        }
    }

    public function setRabbitMQ($setting)
    {
        $this->rabbitmqOpts = $setting;
    }

    public function setQos($qos)
    {
        $this->qos = $qos;
    }

    public function getQos()
    {
        return $this->qos;
    }

    public function setMsgPoolTtl($ttl)
    {
        if (!is_numeric($ttl) && $ttl < 3600) {
            throw new InvalidConfigException('Message Pool Ttl invalid.');
        }

        $this->msgPoolTtl = $ttl;
    }

    public function setMaxRetryTimes($max_retry_times)
    {
        if (!is_numeric($max_retry_times) && $max_retry_times > 999) {
            throw new InvalidConfigException('Max Retry Times invalid.');
        }

        $this->maxRetryTimes = $max_retry_times;
    }

    public function setExchange($exchange)
    {
        $this->exchange = $exchange;
    }

    public function __destruct()
    {
        $this->stop();
    }
}

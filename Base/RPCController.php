<?php
namespace ZJWorkerman\Base;

use ZJPHP\Base\ZJPHP;
use ZJPHP\Base\Controller;
use ZJPHP\Base\Event;
use ZJPHP\Facade\Database;
use ZJPHP\Service\Debugger;
use ZJPHP\Base\Kit\ArrayHelper;
use League\CLImate\CLImate;
use ZJWorkerman\Facade\MQConsumer;
use PDO;
use Exception;

class RPCController extends Controller
{
    /**
     * TO DO:
     * Use reflection to enable named args instead of number index
     * which do not have meanning
     **/
    const RETRY_TIMES = 5; // times
    const WAIT_FOR_DEPENDENCY = 5; // seconds


    public function run()
    {
        $args = func_get_args();
        if (count($args) === 1 && ($args[0] instanceof CLImate)) {
            $args = $this->getCliArgs();
        }
        $connection = $args[0];
        $message = trim($args[1]);

        $envelope = json_decode($message, true);
        $message_signature = $envelope['signature'];
        $message_content = $envelope['content'];

        $rpc = json_decode($message_content, true);
        if (!isset($rpc['action']) ||!isset($rpc['args']) || !method_exists($this, $rpc['action'])) {
            $connection->send(json_encode(['success' => false, 'requeue' => false]));
        }

        try {
            $result = call_user_func_array([$this, $rpc['action']], $rpc['args']);
        } catch (Exception $e) {
            $payload = [
                'error' => $e
            ];
            $event = new Event($payload);
            $debugger = ZJPHP::$app->get('debugger');
            $debugger->trigger(Debugger::EVENT_RUNTIME_ERROR_HAPPEN, $event);
            $result =  ['success' => false, 'requeue' => false];
        }

        if ($result['success']) {
            MQConsumer::internalAck($message_signature);
        }

        $connection->send(json_encode($result));
    }

    protected function getCliArgs($climate)
    {
        // TBD
    }
}

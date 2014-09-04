<?php

ini_set('memory_limit', '1024M');
set_time_limit(0);
error_reporting(E_ALL);
date_default_timezone_set('Europe/Moscow');

require_once __DIR__ . '/vendor/autoload.php';

use Phalcon\CLI\Dispatcher;
use Phalcon\CLI\Task;
use Phalcon\DI\FactoryDefault\CLI;
use Phalcon\CLI\Console;

use Elastica\Client;
use Elastica\ScanAndScroll;
use Elastica\Search;


/**
 * Class backupTask
 *
 * @property \Phalcon\Logger\Adapter log
 * @property \Phalcon\CLI\Dispatcher dispatcher
 */
class backupTask extends Task
{

    /**
     * @var Client
     */
    private $client;

    /**
     * @var \Elastica\Index
     */
    private $elasticaIndex;

    /**
     * @var \Elastica\Type
     */
    private $elasticaType;

    private $fileName;

    private $sizePerShard = 100;

    private $expiryTime = '1m';

    private $folderName;

    protected $indexName;

    protected $typeName;

    /**
     * До любых других действий
     */
    public function initialize()
    {

        $currentActionName = $this->dispatcher->getActiveMethod();

        $annotations = $this->annotations->getMethod(self::class, $currentActionName);

        if ($annotations->has('actionInfo')) {

            $annotation  = $annotations->get('actionInfo');
            $actionTitle = $annotation->getNamedArgument('name');

            $this->log->info('Запустили: {actionTitle}', ['actionTitle' => $actionTitle]);
        } else {
            $currentTaskName = $this->dispatcher->getTaskName();
            $this->log->info(
                'Запустили: {currentTaskName}::{currentActionName}',
                ['currentTaskName' => $currentTaskName, 'currentActionName' => $currentActionName]
            );

        }

        $this->indexName = $this->dispatcher->getParam('index', 'string', false);
        $this->typeName  = $this->dispatcher->getParam('type', 'string', false);

        if (!$this->indexName) {

            $this->log->error('Указание индекса является обязательным параметром');
            die;
        }

        $sizePerShard       = $this->dispatcher->getParam('sizePerShard', 'int', false);
        $this->sizePerShard = $sizePerShard ? $sizePerShard : $this->sizePerShard;


        $this->client        = new Client();
        $this->elasticaIndex = $this->client->getIndex($this->indexName);
        $this->elasticaType  = $this->elasticaIndex->getType($this->typeName);

    }

    public function mainAction()
    {
        $this->log->info('Необходимо выбрать задачу');
    }

    /**
     * @actionInfo(name="Бэкап данных")
     */
    public function backupAction()
    {

        $this->folderName = __DIR__ . '/backup/';

        $this->checkFileName();
        $this->checkBackupFolder();

        $this->log->info(
            'Всё ок, бекапим {indexName} в {fileName}',
            ['indexName' => $this->indexName, 'fileName' => $this->fileName]
        );

        $this->log->info('Параметры бэкапа: sizePerShard={sizePerShard}', ['sizePerShard' => $this->sizePerShard]);

        $scanAndScroll = $this->getScanAndScroll();

        foreach ($scanAndScroll as $resultSet) {

            $buffer = [];

            /* @var \Elastica\ResultSet $resultSet */
            $results = $resultSet->getResults();

            foreach ($results as $result) {

                $item            = [];
                $item['_id']     = $result->getId();
                $item['_source'] = $result->getSource();
                $buffer[]        = json_encode($item, JSON_UNESCAPED_UNICODE);
            }

            $fileBody = implode(PHP_EOL, $buffer);

            if (file_put_contents($this->fileName, $fileBody, FILE_APPEND)) {

                $countDocuments = count($results);
                $this->log->info('Сохранили {countDocuments} записей', ['countDocuments' => $countDocuments]);

            } else {

                $this->log->error('Ошибка записи данных');
                die;
            }
        }
    }

    /**
     * @actionInfo(name="Восстановление данных")
     */
    public function restoreAction()
    {
        $this->log->info('Восстановление');

    }

    /**
     * @return ScanAndScroll
     */
    protected function getScanAndScroll()
    {
        $search = new Search($this->client);
        $search->addIndex($this->elasticaIndex);

        // type не обязателен, бэкапить можно весь индекс со всеми типами
        if ($this->typeName) {

            $search->addType($this->elasticaType);
        }

        $scanAndScroll               = new ScanAndScroll($search);
        $scanAndScroll->sizePerShard = $this->sizePerShard;
        $scanAndScroll->expiryTime   = $this->expiryTime;

        return $scanAndScroll;
    }

    protected function checkFileName()
    {
        $this->fileName = sprintf(
            '%s%s-%s-%s-dump.json',
            $this->folderName,
            $this->indexName,
            ($this->typeName ? $this->typeName : 'all-types'),
            date('Y-m-d-H-i-s', time())
        );
    }

    protected function checkBackupFolder()
    {
        if (!is_dir($this->folderName) && mkdir($this->folderName, 0777, true)) {

            $this->log->info('Каталог бэкапа {folder} отсутствовал, создали его.', ['folder' => $this->folderName]);
        } elseif (is_dir($this->folderName) && is_writable($this->folderName)) {

            $this->log->info(
                'Каталог бэкапа {folder} уже существует и доступен для записи',
                ['folder' => $this->folderName]
            );
        } else {

            $this->log->error(
                'Каталог бэкапа {folder} отсутствует или недоступен для создания',
                ['folder' => $this->folderName]
            );

        }
    }
}

$di = new CLI();

$di['dispatcher'] = function () {

    $dispatcher = new Dispatcher;

    $dispatcher->setDefaultTask('Backup');
    $dispatcher->setDefaultAction('main');

    return $dispatcher;
};

$di['log'] = function () {

    return new Phalcon\Logger\Adapter\Stream('php://stdout');
};

try {

    $console = new Console($di);

    $handleParams = array();
    array_shift($argv);

    foreach ($argv as $param) {
        list($name, $value) = explode('=', $param);
        $handleParams[$name] = $value;
    }

    $console->handle($handleParams);

} catch (\Phalcon\Exception $e) {

    echo $e->getMessage();
    exit(255);
}

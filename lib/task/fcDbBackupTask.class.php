<?php
/**
 * This file is part of the symfony package.
 * (c) Cedric Sadai <cedric@seedweb-agency.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * 
 * Performs an automatic backup of the database. Automatic archiving system.
 *
 * FEATURES:
 *   - Supports multiple snapshots per day
 *     - Keeps all snapshots generated during the current and the previous month.
 *   - Keeps the most recent snapshot of each week among those saved two months ago.
 *   - Keeps the most recent snapshot of the month among the weekly backups saved three months ago.
 *
 * @author Cedric Sadai <cedric@seedweb-agency.com>
 * @link http://www.seedweb-agency.com
 * @version 0.1
 * @license MIT
 *
 *
 */
class fcDbBackupTask extends sfBaseTask
{
    protected $workingDirectory;
    protected $pathToExecutable;
    
    protected function configure()
    {
        $this->addOptions(array(
        new sfCommandOption('application', null, sfCommandOption::PARAMETER_REQUIRED, 'The application name', 'front'),
        new sfCommandOption('env', null, sfCommandOption::PARAMETER_REQUIRED, 'The environment', 'dev'),
        new sfCommandOption('connection', null, sfCommandOption::PARAMETER_REQUIRED, 'The connection name', 'doctrine'),
        // add your own options here
        ));

        $this->namespace        = 'fc';
        $this->name             = 'dbBackup';
        $this->briefDescription = 'Makes a snapshot of the database. Triggers the automatic archiving system.';
        $this->detailedDescription = <<<EOF
        The [fc:dbBackup|INFO] task makes snapshots of the database, for backup purposes.
          - Supports multiple snapshots per day
          - Keeps all snapshots generated during the current and the previous month.
          - Keeps the most recent snapshot of each week among those saved two months ago.
          - Keeps the most recent snapshot of the month among the weekly backups saved three months ago.
        Call it with:

        [php symfony fc:dbBackup|INFO]
EOF;
    }
    
    /**
     * Main callable. Gets the config, does the snapshot, makes the cleaning.
     *
     * @todo make it work not only with DSN, but with all kinds of configs put in database.yml
     * @todo Make it work with more RDBMS
     * @todo Automatic syncing with distant storage services (Amazon S3, etc.)
     *
     *
     */
    protected function execute($arguments = array(), $options = array())
    {
        //-- Fetching the current active database
        $databaseManager  = new sfDatabaseManager($this->configuration);
        $database         = $databaseManager->getDatabase($options['connection'] ? $options['connection'] : null);
        $connection       = $database->getConnection();
        
        //-- Proceeding to configuraiton
        $this->initializeConfig();
        
        //-- Parsing the database connection information
        $dsn  =  $database->getParameter('dsn');
        $name =  $database->getParameter('name');
    
        if ( !strpos($dsn, '://'))
        {
            $dsn = array($dsn, $database->getParameter('username'), $database->getParameter('password'));
        }
        
        $parts = $this->parseDsn($dsn);
        
        //-- preparing the backup command
        $command = $this->getDumpCommand(
            $parts['scheme'], 
            $parts['host'], 
            $parts['user'], 
            $parts['pass'], 
            $parts['path'],
            $parts['socket']
        );
        
        //-- Creating the snapshot
        exec($command);
        $this->logSection('backup', sprintf('Backup done for %s', date('d M Y')));
        
        
        //-- let's do some cleanup
        $this->cleanup();
    }
    
    
    /**
     * Gets the name of the file of the day, accordingly to date x time x sequence.
     *
     *
     */
    protected function getCurrentFilename()
    {
        $file = $finder = sfFinder::type('file')
            ->name(sprintf('%s_*.sql', date('Y-m-d')))
            ->in($this->getDirectory());
            
        $cnt    = count($file) == 0 ? 1 : count($file)+1;
        $cntdef = ($cnt < 10) ? '0' . $cnt : $cnt;
        
        return sprintf('%s/%s_%s.sql', $this->getDirectory(), date('Y-m-d'), $cntdef);
    }
    
    
    /**
     * Parsing the DSN set in databases.yml to make it compatible with both sf1.0 and sf1.2+ writing conventions
     *
     * @param mixed $dsn - The DSN set in databases.yml. Can be array, can be string (Pear-like).
     *
     */
    protected function parseDsn($dsn)
    {
        if (is_array($dsn))
        {
            $dsnParts    =  explode(':', $dsn[0]);
            $decryptDsn  =  array();
            
            foreach (explode(';', $dsnParts[1]) as $val)
            {
                if ($val) // account for possibility of someone ending the string with ;
                {
                  $miniParts = explode('=', $val);
                  $decryptDsn[$miniParts[0]] = $miniParts[1];
                }
            }
            
            if ($dsnParts[0] == 'uri') 
            {
                $dsnParts[0] = 'odbc';
            }
            
            $parts = array(
                'user'   => (isset($dsn[1])) ? $dsn[1] : null,
                'pass'   => (isset($dsn[2])) ? $dsn[2] : null,
                'scheme' => $dsnParts[0],
                'host'   => $decryptDsn['host'],
                'path'   => $decryptDsn['dbname'],
                'socket' => (isset($decryptDsn['unix_socket'])) ? $decryptDsn['unix_socket'] : null,
            );
        }
        else
        {
            $parts = parse_url($dsn);
        }
        
        //-- cleaning starting slash for path (see parse_url behaviour)
        if (preg_match('#^\/#', $parts['path']))
        {
            $parts['path'] = substr($parts['path'], 1);
        }
        
        return $parts;
    }
    
    
    /**
     * Cleans up old backups
     *
     *
     */
    protected function cleanup()
    {
        //-- Timestamps we will be needing.
        $twoMonthsAgo     = mktime(0, 0, 0, date('m')-2, date('d'), date('Y'));
        $threeMonthsAgo   = mktime(0, 0, 0, date('m')-3, date('d'), date('Y'));
        
        //-- If we cleaned that already up, we left a trace
        if (!file_exists($this->getBackupLog($twoMonthsAgo, 'weekly')))
        {
            $this->log('-----------------------------');
            $this->log(sprintf('Weekly Cleaning for %s', date('F Y', $twoMonthsAgo)));
            $this->log('-----------------------------');
            
            //-- Fetching all files, classifying them into weeks, keeping the most recent each week.
            $finder = sfFinder::type('file')
                ->name(sprintf('%s-*.sql', date('Y-m', $twoMonthsAgo)))
                ->in($this->getDirectory());
            
            $weeks = array();
            
            foreach ($finder as $file)
            {
                $week = date('W', $this->getTimestampFromFile($file));
                
                if (array_key_exists($week, $weeks))
                {
                    $old = $weeks[$week];
                    
                    if ($this->getNumericValue($file) > $this->getNumericValue($old))
                    {
                        $weeks[$week] = basename($file);
                        $this->deleteFile($old);
                    }
                    else
                    {
                        $this->deleteFile($file);
                    }
                }
                else
                {
                    $weeks[$week] = basename($file);
                }
            }
            
            
            file_put_contents($this->getBackupLog($twoMonthsAgo, 'weekly'), 'saved');
            $this->logSection('cleaning', sprintf('Weekly cleaning done for %s', date('F Y', $twoMonthsAgo)));
        }
        else
        {
            $this->logSection('cleaning', sprintf('The weekly archive for %s already exists.', date('F Y', $twoMonthsAgo)));
        }
        
        
        
        
        //-- Keeping the most recent backup from all the snapshots made 3 months ago
        if (!file_exists($this->getBackupLog($threeMonthsAgo, 'monthly')))
        {
            if (!file_exists($this->getBackupLog($threeMonthsAgo, 'weekly')))
            {
                $this->logSection('nothing to clean', sprintf('No weekly for %s', date('M Y', $threeMonthsAgo)));
                return;
            }
            else
            {
                $this->log('-----------------------------');
                $this->log(sprintf('Monthly Cleaning for %s', date('F Y', $threeMonthsAgo)));
                $this->log('-----------------------------');
            }
            
            //-- Keeping the highest weekly file.
            $finder = sfFinder::type('file')
                ->name(sprintf('%s-*.sql', date('Y-m', $threeMonthsAgo)))
                ->in($this->getDirectory());
            
            $latest = max($finder);
            
            foreach ($finder as $weekly)
            {
                if ($weekly != $latest)
                {
                    $this->deletefile($weekly);
                }
            }
            
            //-- Leaving our trace
            file_put_contents($this->getBackupLog($threeMonthsAgo, 'monthly'), 'saved');
            $this->logSection('cleaning', sprintf('Monthly cleaning done for %s', date('F Y', $threeMonthsAgo)));
        }
        else
        {
            $this->logSection('passing', sprintf('The archive for %s already exists.', date('F Y', $threeMonthsAgo)));
        }
    }
    
    
    
    /**
     * Gets the backup command, according to the driver
     *
     * @param string $driver
     * @param string $host
     * @param string user
     * @param string pass
     * @param string dbname
     * @todo other RDBMS
     * @todo test it on pgsql (not tested!!)
     *
     *
     */
    protected function getDumpCommand($driver = null, $host = null, $user = null, $pass = null, $dbname = null, $socket = null)
    {
        if ($driver === null || $host === null || $dbname === null || $user === null)
        {
            throw(new sfException('The database parameters seem wrong. Database snapshot failed.'));
            return false;
        }
        
        switch ($driver)
        {
            case 'mysql':
                
                return sprintf('%smysqldump --hex-blob -h %s -u %s -p%s -S%s %s > %s',
                    $this->pathToExecutable,
                    $host,
                    $user,
                    $pass,
                    null === $socket ? '/tmp/mysql.sock' : $socket,
                    $dbname,
                    $this->getCurrentFilename()
                );
                
            break;
            
            
            case 'pgsql':
            
                return sprintf('%spg_dump -h %s -u %s %s %s > %s',
                    $this->pathToExecutable,
                    $host,
                    $user,
                    null === $socket ? '' : '-k '.$socket,
                    $dbname,
                    $this->getCurrentFilename()
                );
            break;
            
            default:
            
                throw(new sfException('Your database management system seems to be unsupported for now.'));
        }
    }
    
    
    /**
     * Grabbing configuration from app.yml, checking and storing preferences.
     *
     *
     */
    protected function initializeConfig()
    {
        //-- Path to store the snapshot
        $path       = sfConfig::get('app_fcDbBackupPlugin_path', null);
        $validPath  = ($path === null) ? null : (preg_match('#(\/|\\\)$#i', $path) ? substr($path, 0, -1) : $path);
        
        //-- Path to RDBMS executable
        $pathToExec             = sfConfig::get('app_fcDbBackupPlugin_pathToExec', '');
        $this->pathToExecutable = ($pathToExec == '') ? '' : (eregi('(\/|\\\)$', $pathToExec) ? $pathToExec : $pathToExec . DIRECTORY_SEPARATOR);
            
            
        if ($validPath !== null && file_exists($validPath))
        {
            $this->workingDirectory = $validPath;
        }
        else
        {
            throw(new sfException('You need to set a valid "path" key under a fcDbBackupPlugin section in your app.yml'));
        }
    }
    
    
    /**
     * Returns the name of the directory where we save everything
     *
     *
     */
    protected function getDirectory()
    {
        return $this->workingDirectory;
    }
    
    
    /**
     * Deletes a file from its name
     *
     * @param string file - The name of the file to delete
     *
     */
    protected function deleteFile($file)
    {
        $toDel = ereg('(\/|\\\)', $file) ? basename($file) : $file;
        $this->logSection('deleting', $toDel);
        unlink($this->getDirectory() . DIRECTORY_SEPARATOR . $toDel);
    }
    
    
    /**
     * Gets the path to the log file
     *
     * @param int $timestamp
     * @param string $type - 'weekly' or 'monthly'
     *
     */
    protected function getBackupLog($timestamp = null, $type = 'weekly')
    {
        return sprintf('%s/%s.saved.%s', $this->getDirectory(), date('Y-m', $timestamp), $type);
    }
    
    
    
    /**
     * Fetches timestamp from file
     *
     * @param string $file - The name of the file to read the timestamp from.
     *
     */
    protected function getTimestampFromFile($file = null)
    {
        if ($file === null)
        {
            return false;
        }
        
        return strToTime(substr(basename($file), 0, strPos(basename($file), '_')));
    }
    
    
    
    /**
     * Converts a timestamped file to numeric value for comparaison
     *
     * @param string $file - The name of the file to get a numeric value from
     *
     */
    protected function getNumericValue($file)
    {
        $toHandle = ereg('(\/|\\\)', $file) ? basename($file) : $file;
        return (int)preg_replace('~[^\\pL\d]+~u', '', $toHandle);
    }
}
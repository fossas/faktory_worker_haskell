-- | High-level interface for a Worker
--
-- Runs forever, @FETCH@-ing Jobs from the given Queue and handing each to your
-- processing function.
--
module Faktory.Worker (
  WorkerHalt (..),
  WorkerConfig (isQuieted, client, workerId, workerSettings, settings),
  runWorker,
  runWorkerEnv,
  withRunWorker,
  quietWorker,
  jobArg,
) where

import Faktory.Prelude
import Control.Concurrent (forkIO, killThread)
import Control.Monad.Reader (MonadIO (..), MonadReader (ask), ReaderT (..))
import Data.Aeson
import Data.Aeson.Casing
import qualified Data.Text as T
import Faktory.Client
import Faktory.Job (Job, JobId, jobArg, jobJid, jobReserveForMicroseconds)
import Faktory.Settings
import GHC.Conc (TVar, atomically, newTVarIO, readTVarIO, writeTVar)
import GHC.Generics
import GHC.Stack
import System.Timeout (timeout)

-- | State information for a faktory worker.
data WorkerConfig = WorkerConfig
  { isQuieted :: TVar Bool
  , client :: Client
  , workerId :: WorkerId
  , workerSettings :: WorkerSettings
  , settings :: Settings
  }

-- | If processing functions @'throw'@ this, @'runWorker'@ will exit
data WorkerHalt = WorkerHalt
  deriving stock (Eq, Show)
  deriving anyclass Exception

newtype BeatPayload = BeatPayload
  { _bpWid :: WorkerId
  }
  deriving stock Generic

instance ToJSON BeatPayload where
  toJSON = genericToJSON $ aesonPrefix snakeCase
  toEncoding = genericToEncoding $ aesonPrefix snakeCase

newtype AckPayload = AckPayload
  { _apJid :: JobId
  }
  deriving stock Generic

instance ToJSON AckPayload where
  toJSON = genericToJSON $ aesonPrefix snakeCase
  toEncoding = genericToEncoding $ aesonPrefix snakeCase

newtype Worker a = Worker
  { runWorkerM :: ReaderT WorkerConfig IO a
  }
  deriving newtype (Functor, Applicative, Monad, MonadReader WorkerConfig, MonadIO, MonadThrow, MonadCatch, MonadMask)

data FailPayload = FailPayload
  { _fpMessage :: Text
  , _fpErrtype :: String
  , _fpJid :: JobId
  , _fpBacktrace :: [String]
  }
  deriving stock Generic

instance ToJSON FailPayload where
  toJSON = genericToJSON $ aesonPrefix snakeCase
  toEncoding = genericToEncoding $ aesonPrefix snakeCase

untilM_ :: Monad m => m Bool -> m a -> m ()
untilM_ predicate action = do
  result <- predicate
  unless result (action *> untilM_ predicate action)

-- | Creates a new faktory worker. @'action'@ is ran with @'WorkerConfig'@ before
-- continuously polls the faktory server for jobs. Jobs are passed to
-- @'handler'@. Polling stops once the worker is quieted.

withRunWorker ::
  (HasCallStack, FromJSON args)
  =>  Settings
  -> WorkerSettings
  -> (Job args -> IO ())
  -> (WorkerConfig -> IO a)
  -> IO ()
withRunWorker settings workerSettings handler action =
  bracket
    (configureWorker settings workerSettings)
    stopWorker
    (action *> runWorkerWithConfig handler)

-- | Creates a new faktory worker and continuously polls the faktory server for
--- jobs which are passed to @'handler'@. Polling stops once the worker is quieted.
runWorker
  :: (HasCallStack, FromJSON args)
  => Settings
  -> WorkerSettings
  -> (Job args -> IO ())
  -> IO ()
runWorker settings workerSettings handler =
  bracket
    (configureWorker settings workerSettings)
    stopWorker
    (runWorkerWithConfig handler)

runWorkerWithConfig :: FromJSON arg => (Job arg -> IO ()) -> WorkerConfig -> IO ()
runWorkerWithConfig handler config = do
  beatThreadId <- forkIO $ forever $ heartBeat config
  finally
    ( flip runReaderT config . runWorkerM $
        untilM_ shouldStopWorker (processorLoop handler)
          `catch` (\(_ex :: WorkerHalt) -> pure ())
    )
    $ killThread beatThreadId

configureWorker :: HasCallStack => Settings -> WorkerSettings -> IO WorkerConfig
configureWorker settings workerSettings = do
  workerId <- maybe randomWorkerId pure $ settingsId workerSettings
  isQuieted <- newTVarIO False
  client <- newClient settings $ Just workerId
  pure $ WorkerConfig{isQuieted, workerId, client, workerSettings, settings}

stopWorker :: WorkerConfig -> IO ()
stopWorker WorkerConfig{client} = closeClient client

runWorkerEnv :: FromJSON args => (Job args -> IO ()) -> IO ()
runWorkerEnv f = do
  settings <- envSettings
  workerSettings <- envWorkerSettings
  runWorker settings workerSettings f

quietWorker :: WorkerConfig -> IO ()
quietWorker WorkerConfig{isQuieted} = do
  liftIO $ atomically $ writeTVar isQuieted True

shouldStopWorker :: Worker Bool
shouldStopWorker = do
  WorkerConfig{isQuieted} <- ask
  liftIO $ readTVarIO isQuieted

processorLoop
  :: (HasCallStack, FromJSON arg)
  => (Job arg -> IO ())
  -> Worker ()
processorLoop f = do
  WorkerConfig{settings, workerSettings} <- ask
  let
    namespace = connectionInfoNamespace $ settingsConnection settings
    processAndAck job = do
      mResult <- liftIO $ timeout (jobReserveForMicroseconds job) $ f job
      case mResult of
        Nothing -> liftIO $ settingsLogError settings "Job reservation period expired."
        Just () -> ackJob job

  emJob <- fetchJob $ namespaceQueue namespace $ settingsQueue
    workerSettings

  case emJob of
    Left err -> liftIO $ settingsLogError settings $ "Invalid Job: " <> err
    Right Nothing -> liftIO $ threadDelaySeconds $ settingsIdleDelay workerSettings
    Right (Just job) ->
      processAndAck job
        `catches` [ Handler $ \(ex :: WorkerHalt) -> throw ex
                  , Handler $ \(ex :: SomeException) ->
                    failJob job $ T.pack $ show ex
                  ]

-- | <https://github.com/contribsys/faktory/wiki/Worker-Lifecycle#heartbeat>
heartBeat :: WorkerConfig -> IO ()
heartBeat WorkerConfig{client, workerId} = do
  threadDelaySeconds 25
  command_ client "BEAT" [encode $ BeatPayload workerId]

fetchJob
  :: FromJSON args => Queue -> Worker (Either String (Maybe (Job args)))
fetchJob queue = do
  WorkerConfig{client} <- ask
  liftIO $ commandJSON client "FETCH" [queueArg queue]

ackJob :: HasCallStack => Job args -> Worker ()
ackJob job = do
  WorkerConfig{client} <- ask
  liftIO $ commandOK client "ACK" [encode $ AckPayload $ jobJid job]

failJob :: HasCallStack => Job args -> Text -> Worker ()
failJob job message = do
  WorkerConfig{client} <- ask
  liftIO $ commandOK client "FAIL" [encode $ FailPayload message "" (jobJid job) []]

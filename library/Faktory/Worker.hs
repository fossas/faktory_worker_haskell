-- | High-level interface for a Worker
--
-- Runs forever, @FETCH@-ing Jobs from the given Queue and handing each to your
-- processing function.
--
module Faktory.Worker
  ( WorkerHalt(..)
  , Worker(..)
  , runWorker
  , runWorkerEnv
  , quietWorker
  , createWorker
  , startWorker
  , jobArg
  )
where

import Faktory.Prelude
import Control.Concurrent (ThreadId, forkIO, killThread, myThreadId)
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

data WorkerConfig = WorkerConfig
  { isQuiet :: TVar Bool
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

forkWithThrowToParent :: Worker () -> Worker ThreadId
forkWithThrowToParent action = do
  parent <- liftIO myThreadId
  workerConfig <- ask
  liftIO $ forkIO $ (flip runReaderT workerConfig. runWorkerM $ action) `catchAny` \err -> throwTo parent err

createWorker
  :: HasCallStack
  => Settings
  -> WorkerSettings
  -> IO WorkerConfig
createWorker settings workerSettings = do
  workerId <- maybe randomWorkerId pure $ settingsId workerSettings
  client <- newClient settings $ Just workerId
  isQuiet <- newTVarIO False
  pure $ WorkerConfig{isQuiet, workerId, client, workerSettings, settings}

runWorker
  :: (HasCallStack, FromJSON args)
  => Settings
  -> WorkerSettings
  -> (Job args -> IO ())
  -> IO ()
runWorker settings workerSettings f = do
  config <- createWorker settings workerSettings
  flip runReaderT config . runWorkerM $ startWorker f

startWorker
  :: (HasCallStack, FromJSON args)
  => (Job args -> IO ())
  -> Worker ()
startWorker f = do
  config <- ask
  liftIO $ flip runReaderT config . runWorkerM $ do
    beatThreadId <- forkWithThrowToParent $ forever heartBeat
    untilM_ shouldRunWorker (processorLoop f)
      `catch` (\(_ex :: WorkerHalt) -> pure ())
      `finally` killWorker beatThreadId

shouldRunWorker :: Worker Bool
shouldRunWorker = do
  WorkerConfig{isQuiet} <- ask
  liftIO $ readTVarIO isQuiet

runWorkerEnv :: FromJSON args => (Job args -> IO ()) -> IO ()
runWorkerEnv f = do
  settings <- envSettings
  workerSettings <- envWorkerSettings
  runWorker settings workerSettings f

quietWorker :: Worker ()
quietWorker = do
  WorkerConfig{isQuiet} <- ask
  liftIO $ atomically $ writeTVar isQuiet True

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
heartBeat :: Worker ()
heartBeat = do
  WorkerConfig{client, workerId} <- ask
  liftIO $ threadDelaySeconds 25
  liftIO $ command_ client "BEAT" [encode $ BeatPayload workerId]

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

untilM_ :: Monad m => m Bool -> m a -> m ()
untilM_ predicate action =
  predicate >>= \a -> unless a (action *> untilM_ predicate action)

killWorker :: ThreadId -> Worker ()
killWorker beatThreadId = do
  WorkerConfig{client} <- ask
  liftIO $ killThread beatThreadId
  liftIO $ closeClient client

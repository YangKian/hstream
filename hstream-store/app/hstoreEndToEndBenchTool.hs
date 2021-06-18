{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception        (finally, mask_)
import           Control.Monad
import           Data.Int                 (Int64)
import           Data.List
import           Data.Map.Strict          as Map
import           Data.Maybe               (fromJust, isJust)
import           Data.String
import           GHC.Stack                (HasCallStack)
import           Options.Applicative
import           System.IO
import           System.Random
import           Text.Printf
import           Z.Data.ASCII             (w2c)
import           Z.Data.CBytes            (CBytes, pack)
import qualified Z.Data.Vector            as ZV
import           Z.IO.Time                (SystemTime (..), getSystemTime')

import           HStream.Store            hiding (info)

logDeviceConfigPath :: CBytes
logDeviceConfigPath = "/data/store/logdevice.conf"

type Timestamp = Int64

reportInterval :: Timestamp
reportInterval = 5000

type DataSize = Int
type TimeDict = MVar (Map DataSize Timestamp)
type Records = MVar [Timestamp]

defaultRecordSet :: [DataSize]
--defaultRecordSet = [1024, 1024 * 10, 1024 * 100, 1024 * 1024]
defaultRecordSet = [1024]

-- TODO: maybe we should make user specific it
recordSize :: Int
recordSize = 1024

recordsInOneLog :: Int
recordsInOneLog = 1024 * 512

perfEndToEnd :: HasCallStack => ParseArgument -> IO ()
perfEndToEnd ParseArgument{..} = do
  let sets = case recordSet of
               [] -> defaultRecordSet
               xs -> xs
  readerClient <- newLDClient logDeviceConfigPath
  writerClient <- newLDClient logDeviceConfigPath
  let name = mkStreamName $ pack streamName
  isExist <- doesStreamExists writerClient name
  when isExist $ do removeStream writerClient name
  putStrLn $ "-----create stream----"
  createStream writerClient name (LogAttrs $ HsLogAttrs 3 Map.empty)
  wLogId <- getCLogIDByStreamName writerClient name
  rLogId <- getCLogIDByStreamName readerClient name
  putStrLn $ "wlogId = " ++ show wLogId ++ " rlogId = " ++ show rLogId

  reader <- newLDReader readerClient 1 Nothing
  readerSetWaitOnlyWhenNoData reader
  readerStartReading reader rLogId LSN_MIN LSN_MAX

  dict <- newEmptyMVar
  records <- newMVar []
--  finally (concurrently (doWrite mvar writerClient logId sets) (doRead mvar reader maxLogs)) (stopReader readerClient reader $ transToStreamName streamName)
  concurrently (doWrite dict writerClient wLogId sets runTimes records) (doRead dict reader maxLogs records)

  putStrLn $ "Done!"

doWrite :: TimeDict -> LDClient -> C_LogID -> [DataSize] -> Int -> Records -> IO ()
doWrite mvar client logId records runs sets = do
  forM_ records $ \size -> do
    replicateM_ runs $ do
      start <- getCurrentTimestamp
      let newDict = Map.insert size start Map.empty
--      putMVar mvar $! newDict
      p <- replicateM size (randomRIO ('a', 'z'))
--      putStrLn $ "================================= " ++ show start ++ " " ++ show (fromString (show start) :: ZV.Bytes)
      let payload = show start ++ p
--      payload <- fromString <$> replicateM size (randomRIO ('a', 'z'))
--      let payload = (show start) : opayload
      _ <- append client logId (fromString payload) Nothing
      printf "Write %.3f MB\n" ((fromIntegral size :: Double) / (1024.0 * 1024.0))
--      putStrLn $ "########## wake up reader"

  threadDelay (120000)
  putStrLn $ "********************************************************************************"
  res <- readMVar sets
  let size = length res
  putStrLn $ "records length = " ++ show size
  let greater100 = Data.List.foldr (\x acc -> if x >= 100 then acc + 1 else acc) 0 res
  putStrLn $ "elsp higher than 100 = " ++ show greater100
  printf "------------------final avg = %.2f ms\n" ((fromIntegral (sum res) :: Double) / (fromIntegral size))

--doRead :: TimeDict -> LDReader -> Int -> Records -> IO ()
--doRead mvar reader maxLogs records = forever $ do
--  res <- readerRead reader 1
--  end <- getCurrentTimestamp
--  putStrLn $ "!!!!res = " ++ show res
--  putStrLn $ "length of res = " ++ show (length res)
--  dict <- takeMVar mvar
--
--  let x = head res
--  let payload = length $ ZV.unpack $ recordPayload x
--  let start = Map.lookup payload dict
--  let recordSize = (fromIntegral payload :: Double) / (1024.0 * 1024.0)
--  case start of
--    Nothing -> putStrLn $ "Can't find the data with size " ++ show recordSize ++ " MB."
--    Just s -> mask_ $ do
--      ls <- takeMVar records
--      putMVar records $ ls ++ [end - s]
--      printf "Finish read data with size %.3f MB, using %d ms\n" recordSize (end - s)
--  putStrLn $ "=====================release mvar"

doRead :: TimeDict -> LDReader -> Int -> Records -> IO ()
doRead mvar reader maxLogs records = forever $ do
  res <- readerRead reader 1
  end <- getCurrentTimestamp
--  putStrLn $ "!!!!res = " ++ show res
--  putStrLn $ "length of res = " ++ show (length res)
--  dict <- takeMVar mvar

  let x = head res
  let payload = length $ ZV.unpack $ recordPayload x
--  let start = Map.lookup payload dict
  let recordSize = (fromIntegral payload :: Double) / (1024.0 * 1024.0)
  let sTime = Data.List.take 13 $ ZV.unpack $ recordPayload x
--  putStrLn $ "==== " ++ show sTime
  let t = Data.List.map w2c sTime
--  putStrLn $ "========= " ++ show t
  mask_ $ do
    ls <- takeMVar records
    putMVar records $ ls ++ [end - (read t)]
  printf "Finish read data with size %.3f MB, using %d ms\n" recordSize (end - (read t))

--  putStrLn $ "=====================release mvar"

getCurrentTimestamp :: IO Timestamp
getCurrentTimestamp = do
  MkSystemTime sec nano <- getSystemTime'
  return $ floor (fromIntegral (sec * 10^3) + (fromIntegral nano / 10^6))

data ParseArgument = ParseArgument
  { streamName :: String
  , recordSet  :: [DataSize]
  , runTimes   :: Int
  , maxLogs    :: Int
  } deriving (Show)

parseConfig :: Parser ParseArgument
parseConfig =
  ParseArgument
    <$> strOption         (long "stream"      <> metavar "STRING"                             <> help "Consumer message from this stream.")
    <*> many (option auto (long "record-size" <> metavar "INT"                                <> help "Message size in bytes. You can specify a variety of record sizes."))
    <*> option auto       (long "run-times"   <> metavar "INT" <> showDefault <> value (1000) <> help "Total numbers of program executions.")
    <*> option auto       (long "max-logs"    <> metavar "INT" <> showDefault <> value (1000) <> help "The amount of logs to fetch in a single request.")

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStore-End-To-End-Bench-Tool")
  putStrLn "HStore-End-To-End-Bench-Tool"
  perfEndToEnd config

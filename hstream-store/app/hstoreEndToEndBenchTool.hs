{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           Control.Monad
import Control.Concurrent
import Control.Concurrent.Async
import           Control.Exception                (finally)
import           Data.Int            (Int64)
import           Data.List
import  Data.Map.Strict     as Map
import           Data.String
import           GHC.Stack           (HasCallStack)
import           Options.Applicative
import           System.Random
import           Text.Printf
import           Z.Data.CBytes       (CBytes, pack)
import           Z.IO.Time           (SystemTime (..), getSystemTime')
import           Data.Maybe                        (fromJust, isJust)
import qualified Z.Data.Vector       as ZV
import System.IO

import           HStream.Store       hiding (info)

logDeviceConfigPath :: CBytes
logDeviceConfigPath = "/data/store/logdevice.conf"

type Timestamp = Int64

reportInterval :: Timestamp
reportInterval = 5000

type DataSize = Int
type TimeDict = MVar (Map DataSize Timestamp)

defaultRecordSet :: [DataSize]
defaultRecordSet = [1024, 1024 * 10, 1024 * 100, 1024 * 1024]

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
  readerStartReading reader rLogId LSN_MIN LSN_MAX

  dict <- newEmptyMVar
--  finally (concurrently (doWrite mvar writerClient logId sets) (doRead mvar reader maxLogs)) (stopReader readerClient reader $ transToStreamName streamName)
  race (doWrite dict writerClient wLogId sets) (doRead dict reader maxLogs)
  putStrLn $ "Done!"

doWrite :: TimeDict -> LDClient -> C_LogID -> [DataSize] -> IO ()
doWrite mvar client logId records = do
  forM_ records $ \size -> do
    start <- getCurrentTimestamp
    let newDict = Map.insert size start Map.empty
    putMVar mvar $! newDict
    payload <- fromString <$> replicateM size (randomRIO ('a', 'z'))
    _ <- append client logId payload Nothing
    printf "Write %.3f MB\n" ((fromIntegral size :: Double) / (1024.0 * 1024.0))
    putStrLn $ "########## wake up reader"

  threadDelay (8000)

doRead :: TimeDict -> LDReader -> Int -> IO ()
doRead mvar reader maxLogs = forever $ do
  res <- readerRead reader 1
  end <- getCurrentTimestamp
  putStrLn $ "length of res = " ++ show (length res)
  dict <- takeMVar mvar

  let x = head res
  let payload = length $ ZV.unpack $ recordPayload x
  let start = Map.lookup payload dict
  let recordSize = (fromIntegral payload :: Double) / (1024.0 * 1024.0)
  case start of
    Nothing -> putStrLn $ "Can't find the data with size " ++ show recordSize ++ " MB."
    Just s -> printf "Finish read data with size %.3f MB, using %d ms\n" recordSize (end - s)
  putStrLn $ "=====================release mvar"

getCurrentTimestamp :: IO Timestamp
getCurrentTimestamp = do
  MkSystemTime sec nano <- getSystemTime'
  return $ floor (fromIntegral (sec * 10^3) + (fromIntegral nano / 10^6))

data ParseArgument = ParseArgument
  { streamName :: String
  , recordSet :: [DataSize]
  , maxLogs    :: Int
  } deriving (Show)

parseConfig :: Parser ParseArgument
parseConfig =
  ParseArgument
    <$> strOption         (long "stream"      <> metavar "STRING"                             <> help "Consumer message from this stream.")
    <*> many (option auto (long "record-size" <> metavar "INT"                                <> help "Message size in bytes. You can specify a variety of record sizes."))
    <*> option auto       (long "run-times"   <> metavar "INT" <> showDefault <> value (1000) <> help "Total numbers of program executions.")

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStore-End-To-End-Bench-Tool")
  putStrLn "HStore-End-To-End-Bench-Tool"
  perfEndToEnd config

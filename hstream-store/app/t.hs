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

import           HStream.Store       hiding (info)

logDeviceConfigPath :: CBytes
logDeviceConfigPath = "/data/store/logdevice.conf"

readTest :: IO ()
readTest = do
--  writerClient <- newLDClient logDeviceConfigPath
  readerClient <- newLDClient logDeviceConfigPath

  let name = mkStreamName "EEEA"

--  createStream writerClient name (LogAttrs $ HsLogAttrs 3 Map.empty)
  rLogId <- getCLogIDByStreamName readerClient name

  reader <- newLDReader readerClient 1 Nothing
  readerStartReading reader rLogId LSN_MIN LSN_MAX

  forever $ do
    res <- readerRead reader 1
    putStrLn $ "------------------res = " ++ show res

main :: IO ()
main = do
  writerClient <- newLDClient logDeviceConfigPath
  let name = mkStreamName "EEEA"
  createStream writerClient name (LogAttrs $ HsLogAttrs 3 Map.empty)
  forkIO $ do
    threadDelay 1000000
--    writerClient <- newLDClient logDeviceConfigPath
--    let name = mkStreamName "DDD"
----    createStream writerClient name (LogAttrs $ HsLogAttrs 3 Map.empty)
    rLogId <- getCLogIDByStreamName writerClient name
    putStrLn $ "write!!!!!!"
    replicateM 5 $ do
      payload <- fromString <$> replicateM 100 (randomRIO ('a', 'z'))
      _ <- append writerClient rLogId payload Nothing
      putStrLn $ "append success"
      threadDelay 1000000
    threadDelay 1000000000000000

--  writerClient <- newLDClient logDeviceConfigPath
--  let name = mkStreamName "DEF"
------  createStream writerClient name (LogAttrs $ HsLogAttrs 3 Map.empty)
--  rLogId <- getCLogIDByStreamName writerClient name
--  putStrLn $ "write!!!!!!"
--  payload <- fromString <$> replicateM 100000 (randomRIO ('a', 'z'))
--  _ <- append writerClient rLogId payload Nothing
--  putStrLn $ "append success"

--  reader <- newLDReader writerClient 1 Nothing
--  readerStartReading reader rLogId LSN_MIN LSN_MAX
--  res <- readerRead reader 100
--  putStrLn $ "res = " ++ show res
--  res1 <- readerRead reader 100
--  putStrLn $ "res1 = " ++ show res1
  readTest
--  threadDelay 10000000000000
{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Werror=incomplete-patterns #-}

module HStream.Server.Handler.Query
  ( -- * For grpc-haskell
    executeQueryHandler
  , executePushQueryHandler
  , terminateQueriesHandler
  , getQueryHandler
  , listQueriesHandler
  , deleteQueryHandler
  , restartQueryHandler
  , createQueryHandler
    -- * For hs-grpc-server
  , handleExecuteQuery
  -- TODO: handleExecutePushQuery
  , handleCreateQuery
  , handleListQueries
  , handleGetQuery
  , handleTerminateQueries
  , handleDeleteQuery
  , handleRestartQuery
  ) where


import           Control.Exception                (Handler (..), catches)
import qualified Data.ByteString.Char8            as BS
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import qualified Data.Vector                      as V
import qualified HsGrpc.Server                    as G
import qualified HsGrpc.Server.Types              as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Query        as Core
import           HStream.Server.Exception         (defaultExHandlers,
                                                   defaultHandlers,
                                                   defaultServerStreamExceptionHandle)
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Types
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

-------------------------------------------------------------------------------

-- Other sqls, called in 'sqlAction'
executeQueryHandler :: ServerContext
                    -> ServerRequest 'Normal API.CommandQuery API.CommandQueryResponse
                    -> IO (ServerResponse 'Normal API.CommandQueryResponse)
executeQueryHandler sc (ServerNormalRequest _metadata req) =
  queryExceptionHandle $ returnResp =<< Core.executeQuery sc req

handleExecuteQuery :: ServerContext -> G.UnaryHandler API.CommandQuery API.CommandQueryResponse
handleExecuteQuery sc _ req = catchQueryEx $ Core.executeQuery sc req

executePushQueryHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming API.CommandPushQuery Struct
  -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler ctx (ServerWriterRequest meta req streamSend) =
  defaultServerStreamExceptionHandle $ do
    Core.executePushQuery ctx req meta streamSend
    returnServerStreamingResp StatusOk ""

-- TODO
-- handleExecutePushQuery

createQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.CreateQueryRequest API.Query
  -> IO (ServerResponse 'Normal API.Query)
createQueryHandler ctx (ServerNormalRequest _metadata req@API.CreateQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Create Query Request with statement: " <> Log.buildText createQueryRequestSql
    Core.createQuery ctx req >>= returnResp

handleCreateQuery
  :: ServerContext -> G.UnaryHandler API.CreateQueryRequest API.Query
handleCreateQuery ctx _ req@API.CreateQueryRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Create Query Request with statement: " <> Log.buildText createQueryRequestSql
  Core.createQuery ctx req

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal API.ListQueriesRequest API.ListQueriesResponse
  -> IO (ServerResponse 'Normal API.ListQueriesResponse)
listQueriesHandler ctx (ServerNormalRequest _metadata _) = queryExceptionHandle $ do
  Log.debug "Receive List Query Request"
  Core.listQueries ctx >>= returnResp . (API.ListQueriesResponse . V.fromList)

handleListQueries
  :: ServerContext -> G.UnaryHandler API.ListQueriesRequest API.ListQueriesResponse
handleListQueries ctx _ _ = catchQueryEx $ do
  Log.debug "Receive List Query Request"
  API.ListQueriesResponse . V.fromList <$> Core.listQueries ctx

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.GetQueryRequest API.Query
  -> IO (ServerResponse 'Normal API.Query)
getQueryHandler ctx (ServerNormalRequest _metadata req@API.GetQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Get Query Request. "
             <> "Query ID: " <> Log.buildText getQueryRequestId
    returnResp =<< Core.getQuery ctx req

handleGetQuery :: ServerContext -> G.UnaryHandler API.GetQueryRequest API.Query
handleGetQuery ctx _ req@API.GetQueryRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Get Query Request. "
           <> "Query ID: " <> Log.buildText getQueryRequestId
  Core.getQuery ctx req

terminateQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal API.TerminateQueriesRequest API.TerminateQueriesResponse
  -> IO (ServerResponse 'Normal API.TerminateQueriesResponse)
terminateQueriesHandler ctx (ServerNormalRequest _metadata req@API.TerminateQueriesRequest{..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueriesRequestQueryId)
  returnResp =<< Core.terminateQueries ctx req

handleTerminateQueries
  :: ServerContext -> G.UnaryHandler API.TerminateQueriesRequest API.TerminateQueriesResponse
handleTerminateQueries ctx _ req@API.TerminateQueriesRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueriesRequestQueryId)
  Core.terminateQueries ctx req

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ctx (ServerNormalRequest _metadata req@API.DeleteQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Delete Query Request. "
      <> "Query ID: " <> Log.buildText deleteQueryRequestId
    Core.deleteQuery ctx req
    returnResp Empty

handleDeleteQuery :: ServerContext -> G.UnaryHandler API.DeleteQueryRequest Empty
handleDeleteQuery ctx _ req@API.DeleteQueryRequest{..} = catchQueryEx $ do
  Log.debug $ "Receive Delete Query Request. "
           <> "Query ID: " <> Log.buildText deleteQueryRequestId
  Core.deleteQuery ctx req
  pure Empty

-- FIXME: Incorrect implementation!
restartQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal API.RestartQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
restartQueryHandler _ (ServerNormalRequest _metadata _) = do
  Log.fatal "Restart Query Not Supported"
  returnErrResp StatusUnimplemented "restart query not suppported yet"
    -- queries <- P.withMaybeZHandle metaHandle P.getQueries
    -- case find (\P.PersistentQuery{..} -> cBytesToLazyText queryId == restartQueryRequestId) queries of
    --   Just query -> do
    --     P.withMaybeZHandle metaHandle $ P.setQueryStatus (P.queryId query) P.Running
    --     returnResp Empty
      -- Nothing    -> returnErrResp StatusInternal "Query does not exist"

handleRestartQuery
  :: ServerContext -> G.UnaryHandler API.RestartQueryRequest Empty
handleRestartQuery _ _ _ = undefined

--------------------------------------------------------------------------------
-- Exception and Exception Handler

sqlExceptionHandlers :: [HE.Handler (StatusCode, StatusDetails)]
sqlExceptionHandlers = [
  Handler (\(err :: SomeSQLException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInvalidArgument, StatusDetails . BS.pack . formatSomeSQLException $ err))
  ]

sqlExHandlers :: [Handler a]
sqlExHandlers =
  [ Handler $ \(err :: SomeSQLException) -> do
      Log.warning $ Log.buildString' err
      let errmsg = BS.pack . formatSomeSQLException $ err
      G.throwGrpcError $ G.GrpcStatus G.StatusInvalidArgument (Just errmsg) Nothing
  ]

queryExceptionHandle :: HE.ExceptionHandle (ServerResponse 'Normal a)
queryExceptionHandle = HE.mkExceptionHandle . HE.setRespType mkServerErrResp $
  sqlExceptionHandlers ++ defaultHandlers

catchQueryEx :: IO a -> IO a
catchQueryEx action = action `catches` (sqlExHandlers ++ defaultExHandlers)

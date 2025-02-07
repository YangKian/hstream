{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Action
  ( Action

  , createStream
  , createStreamBySelect
  , createConnector
  , createSubscription
  , createSubscription'
  , describeCluster
  , dropAction
  , deleteSubscription
  , insertIntoStream
  , listConnectors
  , listStreams
  , listQueries
  , listShards
  , lookupResource
  , pauseConnector
  , resumeConnector
  , listSubscriptions
  , listViews
  , terminateQueries
  ) where

import qualified Data.ByteString                  as BS
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word64)
import           Network.GRPC.HighLevel.Generated (ClientResult (..),
                                                   GRPCMethodType (Normal))
import qualified Proto3.Suite                     as PT
import           Proto3.Suite.Class               (def)

import           HStream.Client.Types             (ResourceType (..))
import           HStream.Client.Utils
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL.AST                  (StreamName)
import           HStream.SQL.Codegen              (DropObject (..),
                                                   InsertType (..),
                                                   TerminationSelection (..))
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils

type Action a = HStreamClientApi -> IO (ClientResult 'Normal a)

createStream :: StreamName -> Int
  -> Action API.Stream
createStream sName rFac API.HStreamApi{..} =
  hstreamApiCreateStream (mkClientNormalRequest' def
    { API.streamStreamName        = sName
    , API.streamReplicationFactor = fromIntegral rFac
    , API.streamShardCount        = 1})

listStreams :: Action API.ListStreamsResponse
listStreams    API.HStreamApi{..} = hstreamApiListStreams clientDefaultRequest
listViews   :: Action API.ListViewsResponse
listViews      API.HStreamApi{..} = hstreamApiListViews clientDefaultRequest
listQueries :: Action API.ListQueriesResponse
listQueries    API.HStreamApi{..} = hstreamApiListQueries clientDefaultRequest
listConnectors :: Action API.ListConnectorsResponse
listConnectors API.HStreamApi{..} = hstreamApiListConnectors clientDefaultRequest
listSubscriptions :: Action API.ListSubscriptionsResponse
listSubscriptions API.HStreamApi{..} = hstreamApiListSubscriptions clientDefaultRequest

terminateQueries :: TerminationSelection
  -> HStreamClientApi
  -> IO (ClientResult 'Normal API.TerminateQueriesResponse )
terminateQueries (OneQuery qid) API.HStreamApi{..} =
  hstreamApiTerminateQueries
    (mkClientNormalRequest' def{API.terminateQueriesRequestQueryId = V.singleton qid})
terminateQueries AllQueries API.HStreamApi{..} =
  hstreamApiTerminateQueries
    (mkClientNormalRequest' def{API.terminateQueriesRequestAll = True})
terminateQueries (ManyQueries qids) API.HStreamApi{..} =
  hstreamApiTerminateQueries
    (mkClientNormalRequest'
      def {API.terminateQueriesRequestQueryId = V.fromList qids})

dropAction :: Bool -> DropObject -> Action Empty
dropAction ignoreNonExist dropObject API.HStreamApi{..}  = do
  case dropObject of
    DStream    txt -> hstreamApiDeleteStream (mkClientNormalRequest' def
                      { API.deleteStreamRequestStreamName     = txt
                      , API.deleteStreamRequestIgnoreNonExist = ignoreNonExist
                      , API.deleteStreamRequestForce          = True
                      })

    DView      txt -> hstreamApiDeleteView (mkClientNormalRequest' def
                      { API.deleteViewRequestViewId = txt
                      , API.deleteViewRequestIgnoreNonExist = ignoreNonExist
                      })

    DConnector txt -> hstreamApiDeleteConnector (mkClientNormalRequest' def
                      { API.deleteConnectorRequestName = txt
                      -- , API.deleteConnectorRequestIgnoreNonExist = checkIfExist
                      })

insertIntoStream
  :: StreamName -> Word64 -> InsertType -> BS.ByteString
  -> Action API.AppendResponse
insertIntoStream sName shardId insertType payload API.HStreamApi{..} = do
  let header = case insertType of
        JsonFormat -> buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty clientDefaultKey
        RawFormat  -> buildRecordHeader API.HStreamRecordHeader_FlagRAW Map.empty clientDefaultKey
      hsRecord = mkHStreamRecord header payload
      record = mkBatchedRecord (PT.Enumerated (Right CompressionTypeNone)) Nothing 1 (V.singleton hsRecord)
  hstreamApiAppend (mkClientNormalRequest' def
    { API.appendRequestShardId    = shardId
    , API.appendRequestStreamName = sName
    , API.appendRequestRecords    = Just record
    })

createStreamBySelect :: String -> Action API.Query
createStreamBySelect sql API.HStreamApi{..} =
  hstreamApiCreateQuery (mkClientNormalRequest' def
    { API.createQueryRequestSql = T.pack sql})

createConnector :: String -> Action API.Connector
createConnector sql API.HStreamApi{..} =
  hstreamApiCreateConnector (mkClientNormalRequest' def
    { API.createConnectorRequestSql = T.pack sql})


listShards :: T.Text -> Action API.ListShardsResponse
listShards sName API.HStreamApi{..} = do
  hstreamApiListShards $ mkClientNormalRequest' def {
    listShardsRequestStreamName = sName
  }

lookupResource :: ResourceType -> Action (Maybe API.ServerNode)
lookupResource (ResSubscription sid) API.HStreamApi{..} =
  fakeMap API.lookupSubscriptionResponseServerNode <$> hstreamApiLookupSubscription
    (mkClientNormalRequest' def { lookupSubscriptionRequestSubscriptionId = sid })
lookupResource (ResShard sid) API.HStreamApi{..} =
  fakeMap API.lookupShardResponseServerNode <$> hstreamApiLookupShard
    (mkClientNormalRequest' def { lookupShardRequestShardId = sid })
lookupResource (ResConnector cid) API.HStreamApi{..}  =
  fakeMap API.lookupConnectorResponseServerNode <$> hstreamApiLookupConnector
    (mkClientNormalRequest' def { lookupConnectorRequestName = cid })

describeCluster :: Action API.DescribeClusterResponse
describeCluster API.HStreamApi{..} = hstreamApiDescribeCluster clientDefaultRequest

pauseConnector :: T.Text -> Action Empty
pauseConnector cid HStreamApi{..} = hstreamApiPauseConnector $
  mkClientNormalRequest' def { pauseConnectorRequestName = cid }

resumeConnector :: T.Text -> Action Empty
resumeConnector cid HStreamApi{..} = hstreamApiResumeConnector $
  mkClientNormalRequest' def { resumeConnectorRequestName = cid }

createSubscription :: T.Text -> T.Text -> Action Subscription
createSubscription subId sName = createSubscription' (subscriptionWithDefaultSetting subId sName)

createSubscription' :: Subscription -> Action Subscription
createSubscription' sub HStreamApi{..} = hstreamApiCreateSubscription $ mkClientNormalRequest' sub

deleteSubscription :: T.Text -> Bool -> Action Empty
deleteSubscription subId force HStreamApi{..} = hstreamApiDeleteSubscription $
  mkClientNormalRequest' def { deleteSubscriptionRequestSubscriptionId = subId
                             , deleteSubscriptionRequestForce = force}

--------------------------------------------------------------------------------

fakeMap :: (a -> b) -> ClientResult 'Normal a -> ClientResult 'Normal b
fakeMap f (ClientNormalResponse x _meta1 _meta2 _status _details) =
  ClientNormalResponse (f x) _meta1 _meta2 _status _details
fakeMap _ (ClientErrorResponse err) = ClientErrorResponse err

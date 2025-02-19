{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Monad
import           Data.Aeson              (Value (..))
import           Data.Maybe              (fromJust)
import           Data.Word

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types
import qualified HStream.Utils.Aeson     as A

main :: IO ()
main = do
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec
      (builder_3, node_2) = addNode builder_2 subgraph_0 InputSpec
  let (builder_4, node_1') = addNode builder_3 subgraph_0 (IndexSpec node_1)
      (builder_5, node_2') = addNode builder_4 subgraph_0 (IndexSpec node_2)


  let joinCond = \row1 row2 -> (A.!) row1 "b" == (A.!) row2 "b"
      rowgen o1 o2 = A.fromList [ ("a", (A.!) o1 "a")
                                , ("b", (A.!) o1 "b")
                                , ("c", (A.!) o2 "c")
                                ]
      joinType = MergeJoinInner
      nullRowgen = \row -> A.fromList (map (\(k,v) -> (k,Null)) (A.toList row))
  let (builder_6, node_3) = addNode builder_5 subgraph_0 (JoinSpec node_1' node_2' joinType joinCond (Joiner rowgen) nullRowgen)

  let (builder_7, node_4) = addNode builder_6 subgraph_0 (OutputSpec node_3)

  let graph = buildGraph builder_7

  shard <- buildShard graph
  stop_m <- newEmptyMVar
  forkIO $ run shard stop_m
  forkIO . forever $ popOutput shard node_4 (\dcb -> print $ "---> Output DataChangeBatch: " <> show dcb)

  pushInput shard node_2
    (DataChange (A.fromList [("b", Number 2), ("c", Number 3)]) (Timestamp (1 :: Word32) []) 1 0)

  flushInput shard node_2
  advanceInput shard node_2 (Timestamp 6 [])

  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (1 :: Word32) []) 1 1)
  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (2 :: Word32) []) 1 2)
  flushInput shard node_1
  advanceInput shard node_1 (Timestamp 3 [])

  threadDelay 1000000

  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (4 :: Word32) []) 1 3)
  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (5 :: Word32) []) 1 4)
  advanceInput shard node_1 (Timestamp 6 [])

  threadDelay 10000000

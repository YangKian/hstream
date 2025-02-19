module HStream.RqliteUtilsSpec where

import qualified Data.Aeson                    as A
import qualified Data.ByteString.Lazy          as BL
import qualified Data.Map.Strict               as Map
import           Data.Maybe                    (fromMaybe)
import qualified Data.Text                     as T
import           Network.HTTP.Client           (Manager, defaultManagerSettings,
                                                newManager)
import           System.Environment            (lookupEnv)
import           Test.Hspec
import           Test.QuickCheck

import qualified HStream.Logger                as Log
import           HStream.MetaStore.RqliteUtils (Id, ROp (..), createTable,
                                                deleteFrom, deleteTable,
                                                insertInto, selectFrom,
                                                transaction, updateSet, upsert)
import           HStream.TestUtils             (AB (..), MetaExample (..), name)

spec :: Spec
spec = do
  runIO $ Log.setLogLevel (Log.Level Log.DEBUG) True
  m <- runIO $ newManager defaultManagerSettings
  port <- runIO $ fromMaybe "4001" <$> lookupEnv "RQLITE_LOCAL_PORT"
  let host = "localhost"
  let url = T.pack $ host <> ":" <> port

  it "Smoke Test" $ do
    table <- generate name
    let v  = AB { a = "EXAMPLE-VALUE", b =1}
    let v2 = AB { a = "EXAMPLE-VALUE-2", b =2}
    createTable m url table
    let id_1 = "my-id-1"
    let id_2 = "my-id-2"
    insertInto  m url table id_1 v
    upsert      m url table id_2 v
    selectFrom  m url table Nothing     `shouldReturn` Map.fromList [(id_1, (v, 0)), (id_2, (v, 0))]
    updateSet   m url table id_1 v2 Nothing
    upsert      m url table id_2 v2
    selectFrom  m url table Nothing     `shouldReturn` Map.fromList [(id_1, (v2, 1)), (id_2, (v2, 1))]
    deleteFrom  m url table (Just id_1) Nothing
    deleteFrom  m url table (Just id_2) Nothing
    selectFrom  m url table (Just id_1)     `shouldReturn` (mempty :: Map.Map Id (AB, Int))
    selectFrom  m url table Nothing     `shouldReturn` (mempty :: Map.Map Id (AB, Int))
    deleteTable m url table

  it "MultiOp Smoke Test" $ do
    table <- generate name
    let v  = AB { a = "EXAMPLE-VALUE", b = 1}
    let v2 = AB { a = "EXAMPLE-VALUE-2", b =2}
    let vBS = BL.toStrict $ A.encode v
    let v2BS = BL.toStrict $ A.encode v2
    let id1 = "my-id-1" :: T.Text
    let id2 = "my-id-2" :: T.Text
    let opInsert =
         [ InsertROp table id1 vBS
         , InsertROp table id2 vBS]
    let opUpdateFail =
         [ CheckROp  table id1 2
         , UpdateROp table id1 v2BS
         , UpdateROp table id2 v2BS]
    let opUpdate =
         [ CheckROp  table id1 0
         , UpdateROp table id1 v2BS
         , UpdateROp table id2 v2BS]
    let opDelete =
         [ DeleteROp table id1
         , DeleteROp table id2 ]

    createTable m url table
    -- Insert
    transaction m url opInsert
    selectFrom m url table (Just id1) `shouldReturn` Map.singleton id1 (v, 0)
    selectFrom m url table (Just id2) `shouldReturn` Map.singleton id2 (v, 0)

    -- Update check fail
    transaction m url opUpdateFail `shouldThrow` anyException
    selectFrom m url table (Just id1) `shouldReturn` Map.singleton id1 (v, 0)
    selectFrom m url table (Just id2) `shouldReturn` Map.singleton id2 (v, 0)

    -- Update
    transaction m url opUpdate
    selectFrom m url table (Just id1) `shouldReturn` Map.singleton id1 (v2, 1)
    selectFrom m url table (Just id2) `shouldReturn` Map.singleton id2 (v2, 1)

    -- Delete
    transaction m url opDelete
    selectFrom m url table (Just id1) `shouldReturn` (mempty :: Map.Map Id (AB, Int))
    selectFrom m url table (Just id2) `shouldReturn` (mempty :: Map.Map Id (AB, Int))
    deleteTable m url table

  aroundAll (runWithUrlAndTable m url) $ do
    describe "Detailed test" $ do
      it "Insert into table with random data" $ \table -> do
        meta@Meta{..} <- generate arbitrary
        insertInto m url table metaId meta
        selectFrom m url table (Just metaId) `shouldReturn` Map.singleton metaId (meta, 0)

      it "Update with random data" $ \table -> do
        meta@Meta{..} <- generate arbitrary
        putStrLn "Update Empty"
        updateSet m url table metaId meta Nothing `shouldThrow` anyException
        insertInto m url table metaId meta
        selectFrom m url table (Just metaId) `shouldReturn` Map.singleton metaId (meta, 0)

        putStrLn "Update with version"
        meta2 <- generate (arbitrary :: Gen MetaExample)
        updateSet m url table metaId meta2 (Just 0)
        selectFrom m url table (Just metaId) `shouldReturn` Map.singleton metaId (meta2, 1)

        putStrLn "Update with no version"
        updateSet m url table metaId meta Nothing
        selectFrom m url table (Just metaId) `shouldReturn` Map.singleton metaId (meta, 2)

        putStrLn "Update with invalid version"
        updateSet m url table metaId meta2 (Just 1) `shouldThrow` anyException
        selectFrom m url table (Just metaId) `shouldReturn` Map.singleton metaId (meta, 2)

      it "Delete from with id" $ \table -> do
        meta@Meta{..} <- generate arbitrary

        putStrLn "Delete non-existing id"
        deleteFrom m url table (Just metaId) Nothing `shouldThrow` anyException

        insertInto m url table metaId meta
        selectFrom m url table (Just metaId) `shouldReturn` Map.singleton metaId (meta, 0)

        putStrLn "Delete with wrong version"
        deleteFrom m url table (Just metaId) (Just 10) `shouldThrow` anyException
        selectFrom m url table (Just metaId) `shouldReturn` Map.singleton metaId (meta, 0)

        putStrLn "Delete with no version"
        deleteFrom m url table (Just metaId) Nothing
        selectFrom m url table (Just metaId) `shouldReturn` (mempty :: Map.Map Id (MetaExample, Int))

        putStrLn "Delete with version"

        insertInto m url table metaId meta
        selectFrom m url table (Just metaId) `shouldReturn` Map.singleton metaId (meta, 0)

        deleteFrom m url table (Just metaId) (Just 0)
        selectFrom m url table (Just metaId) `shouldReturn` (mempty :: Map.Map Id (MetaExample, Int))


runWithUrlAndTable :: Manager -> T.Text -> ActionWith T.Text -> IO ()
runWithUrlAndTable manager url action = do
  tableName <- generate name
  createTable manager url tableName
  action tableName
  deleteTable manager url tableName

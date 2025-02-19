module HStream.Client.Types where

import           Control.Concurrent            (MVar)
import           Data.ByteString               (ByteString)
import           Data.Text                     (Text)
import           Data.Word                     (Word32, Word64)
import           Network.GRPC.HighLevel.Client (ClientSSLConfig)
import qualified Options.Applicative           as O

import           HStream.Admin.Server.Types    (StreamCommand,
                                                SubscriptionCommand,
                                                streamCmdParser,
                                                subscriptionCmdParser)
import           HStream.Server.Types          (ServerID)
import           HStream.Utils                 (SocketAddr)

data HStreamCommand = HStreamCommand
  { cliConnOpts :: CliConnOpts
  , cliCommand  :: Command
  }

data Command
  = HStreamSql HStreamSqlOpts
  | HStreamNodes HStreamNodes
  | HStreamInit HStreamInitOpts
  | HStreamStream StreamCommand
  | HStreamSubscription SubscriptionCommand

commandParser :: O.Parser HStreamCommand
commandParser = HStreamCommand
  <$> connOptsParser
  <*> O.hsubparser
    (  O.command "sql"   (O.info (HStreamSql <$> hstreamSqlOptsParser) (O.progDesc "Start HStream SQL Shell"))
    <> O.command "nodes" (O.info (HStreamNodes <$> hstreamNodesParser) (O.progDesc "Manage HStream Server Cluster"))
    <> O.command "init"  (O.info (HStreamInit <$> hstreamInitOptsParser ) (O.progDesc "Init HStream Server Cluster"))
    <> O.command "stream"        (O.info (HStreamStream <$> streamCmdParser ) (O.progDesc "Manage Streams in HStreamDB"))
    <> O.command "subscription"  (O.info (HStreamSubscription <$> subscriptionCmdParser) (O.progDesc "Manage Subscriptions in HStreamDB"))
    )

data HStreamSqlContext = HStreamSqlContext
  { availableServers :: MVar [SocketAddr]
  , currentServer    :: MVar SocketAddr
  , updateInterval   :: Int
  , sslConfig        :: Maybe ClientSSLConfig
  }

data HStreamSqlOpts = HStreamSqlOpts
  { _updateInterval :: Int
  , _retryTimeout   :: Int
  , _execute        :: Maybe String
  , _historyFile    :: Maybe FilePath
  }

data ResourceType
  = ResSubscription Text
  | ResShard Word64
  | ResConnector Text
  deriving (Show, Eq)

hstreamSqlOptsParser :: O.Parser HStreamSqlOpts
hstreamSqlOptsParser = HStreamSqlOpts
  <$> O.option O.auto (O.long "update-interval" <> O.metavar "INT" <> O.showDefault <> O.value 30 <> O.help "interval to update available servers in seconds")
  <*> O.option O.auto (O.long "retry-timeout"   <> O.metavar "INT" <> O.showDefault <> O.value 60 <> O.help "timeout to retry connecting to a server in seconds")

  <*> (O.optional . O.option O.str) (O.long "execute" <> O.short 'e' <> O.metavar "STRING" <> O.help "execute the statement and quit")
  <*> (O.optional . O.option O.str) (O.long "history-file" <> O.metavar "STRING" <> O.help "history file path to write interactively executed statements")

data HStreamNodes
  = HStreamNodesList
  | HStreamNodesStatus (Maybe ServerID)
  | HStreamNodesCheck (Maybe Word32)

hstreamNodesParser :: O.Parser HStreamNodes
hstreamNodesParser = O.hsubparser
  (  O.command "list"     (O.info (pure HStreamNodesList) (O.progDesc "List all running nodes in the cluster"))
  <> O.command "status"   (O.info (HStreamNodesStatus <$> (O.optional . O.option O.auto) (O.long "id" <> O.help "Specify the id of the node"))
                                  (O.progDesc "Show the status of nodes specified, if not specified show the status of all nodes"))
  <> O.command "check-running" (O.info (HStreamNodesCheck <$> (O.optional . O.option O.auto) (O.long "minimum-running" <> O.short 'n' <> O.help "Specify minimum number of the nodes") )
                                       (O.progDesc "Check if all nodes in the the cluster are running, and the number of nodes is at least as specified"))
  )

newtype HStreamInitOpts = HStreamInitOpts { _timeoutSec :: Int }

hstreamInitOptsParser :: O.Parser HStreamInitOpts
hstreamInitOptsParser = HStreamInitOpts
 <$> O.option O.auto (O.long "timeout" <> O.metavar "INT" <> O.showDefault <> O.value 5 <> O.help "timeout for the wait of cluster ready")

data CliConnOpts = CliConnOpts
  { _serverHost :: ByteString
  , _serverPort :: Int
  , _tlsCa      :: Maybe FilePath
  , _tlsKey     :: Maybe FilePath
  , _tlsCert    :: Maybe FilePath
  } deriving (Show, Eq)

serverHost :: O.Parser ByteString
serverHost =
  O.strOption ( O.long "host" <> O.metavar "SERVER-HOST"
              <> O.showDefault <> O.value "127.0.0.1"
              <> O.help "Server host value"
              )

serverPort :: O.Parser Int
serverPort =
  O.option O.auto ( O.long "port" <> O.metavar "INT"
                  <> O.showDefault <> O.value 6570
                  <> O.help "Server port value"
                  )

connOptsParser :: O.Parser CliConnOpts
connOptsParser = CliConnOpts
  <$> serverHost
  <*> serverPort
  <*> (O.optional . O.option O.str) (O.long "tls-ca"   <> O.metavar "STRING" <> O.help "path name of the file that contains list of trusted TLS Certificate Authorities")
  <*> (O.optional . O.option O.str) (O.long "tls-key"  <> O.metavar "STRING" <> O.help "path name of the client TLS private key file")
  <*> (O.optional . O.option O.str) (O.long "tls-cert" <> O.metavar "STRING" <> O.help "path name of the client TLS public key certificate file")

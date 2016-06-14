{-# LANGUAGE OverloadedStrings #-}
module Main
    ( main
    ) where

import Control.Concurrent (threadDelay)
import Control.Monad (void, forM_, forever)
import Control.Concurrent.STM ( STM
                              , TVar
                              , atomically
                              , newTVarIO
                              , readTVar
                              , writeTVar
                              )
import Data.ByteString (ByteString)
import Data.Map.Lazy (Map)
import Network.Nats
import System.Environment (getArgs)

import qualified Data.ByteString.Char8 as BS
import qualified Data.Map.Lazy as Map

import Types (ServiceConfig (..))

data Entry
    = Entry !ServiceConfig 
    | Pending ![Topic]
    deriving Show

type ServiceMap = Map ByteString Entry

main :: IO ()
main = do
    args <- getArgs
    case args of
        [natsUri] -> do
            putStrLn natsUri
            let settings = defaultSettings { loggerSpec = StdoutLogger }
            runNatsClient settings (BS.pack natsUri) serviceDirectory

        _         -> putStrLn "Usage: service-directory <NatsURI>"

serviceDirectory :: Connection -> IO ()
serviceDirectory conn = do
    sm <- newTVarIO Map.empty
    void $ subAsyncJson' conn "service.register.*" $ serviceRegister conn sm
    void $ subAsync' conn "service.request.*" $ serviceRequest conn sm

    forever $ threadDelay 10000000

serviceRegister :: Connection -> TVar ServiceMap
                -> JsonMsg ServiceConfig -> IO ()
serviceRegister conn sm (JsonMsg topic _ _ (Just conf)) = do
    let [_, _, service] = BS.split '.' topic
    xs <- atomically $ insertService sm service conf
    forM_ xs $ \reply -> pubJson' conn reply conf

serviceRegister _ _ (JsonMsg _ _ _ Nothing) =
    putStrLn "serviceRegister: Cannot decode JSON"

insertService :: TVar ServiceMap -> ByteString -> ServiceConfig
              -> STM [Topic]
insertService sm service conf = do
    sm' <- readTVar sm
    case Map.lookup service sm' of
        Just (Pending xs) -> do
            writeTVar sm $ Map.insert service (Entry conf) sm'
            return xs
        _                 -> do
            writeTVar sm $ Map.insert service (Entry conf) sm'
            return []

serviceRequest :: Connection -> TVar ServiceMap -> NatsMsg -> IO ()
serviceRequest conn sm (NatsMsg topic _ (Just reply) _) = do
    let [_, _, service] = BS.split '.' topic
    mConf <- atomically $ requestConf sm service reply
    maybe (return ()) (pubJson' conn reply) mConf

serviceRequest _ _ (NatsMsg _ _ Nothing _) =
    putStrLn "serviceRequest: No reply topic"

requestConf :: TVar ServiceMap -> ByteString -> Topic 
            -> STM (Maybe ServiceConfig)
requestConf sm service reply = do
    sm' <- readTVar sm
    case Map.lookup service sm' of
        Just (Entry conf) -> return (Just conf)
        Just (Pending xs) -> do
            writeTVar sm $ Map.insert service (Pending (reply:xs)) sm'
            return Nothing
        Nothing           -> do
            writeTVar sm $ Map.insert service (Pending [reply]) sm'
            return Nothing


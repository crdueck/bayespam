import Control.Arrow
import Control.Monad
import Data.List
import Data.Maybe

import Database.HDBC
import Database.HDBC.Sqlite3
import System.Directory
import System.Environment

import qualified Data.Text as T
import qualified Data.Map  as M

type Word       = T.Text
type Dictionary = M.Map Word Double
newtype Spam    = Spam { getSpam :: [Word] }
newtype Mail    = Mail { getMail :: [Word] }

class Spamacity a where
    spamacity :: Dictionary -> a -> Double

    isSpam :: Dictionary -> a -> Bool
    isSpam = ((>=0.9) .) . spamacity

instance Spamacity T.Text where
    spamacity dict word = fromMaybe 0.5 $ M.lookup word dict

instance Spamacity Mail where
    spamacity dict (Mail m) = bayes (product ps) (product qs)
        where ps = [spamacity dict w | w <- m]
              qs = [1 - p | p <- ps]

bayes :: Fractional a => a -> a -> a
bayes x y = x / (x + y)

buildDictionary :: Spam -> Mail -> Dictionary
buildDictionary (Spam s) (Mail m) = M.fromList $ zipWith prob (freq s) (freq m)
    where freq = map (genericLength &&& head) . group . sort
          prob (o1, w) (o2, _) = (w, bayes (o1 / genericLength s) (o2 / genericLength m))

parseFile :: FilePath -> IO [Word]
parseFile = liftM (T.words . T.pack) . readFile

getFiles :: FilePath -> IO [Word]
getFiles = getDirectoryContents >=> liftM join . mapM parseFile

runDictionary :: FilePath -> FilePath -> (FilePath -> IO [Word]) -> IO Dictionary
runDictionary spam mail func = do
    spamWords <- liftM Spam $ func spam
    mailWords <- liftM Mail $ func mail
    return $ buildDictionary spamWords mailWords

dictToSql :: Dictionary -> [[SqlValue]]
dictToSql = map (tupleToList . (toSql *** toSql)) . M.toList
    where tupleToList (k, a) = [k, a]

sqlToDict :: [[SqlValue]] -> Dictionary
sqlToDict = M.fromList . map ((fromSql *** fromSql) . listToTuple)
    where listToTuple ~[k, a] = (k, a)

createSpamTable :: FilePath -> IO ()
createSpamTable fp = do
    conn <- connectSqlite3 fp
    _    <- run conn "CREATE TABLE spam (word TEXT PRIMARY KEY, spamacity FLOAT);" []
    commit conn
    disconnect conn

buildSpamTable :: FilePath -> Dictionary -> IO ()
buildSpamTable fp dict = do
    conn <- connectSqlite3 fp
    stmt <- prepare conn "INSERT OR REPLACE INTO spam VALUES (?, ?)"
    executeMany stmt $ dictToSql dict
    commit conn
    disconnect conn

readDictionary :: FilePath -> IO Dictionary
readDictionary fp = do
    conn <- connectSqlite3 fp
    liftM sqlToDict $ quickQuery conn "SELECT word, spamacity from spam" []

createDictionary :: FilePath -> FilePath -> IO Dictionary
createDictionary spam mail = do
    dir <- liftM2 (&&) (doesDirectoryExist spam) (doesDirectoryExist mail)
    runDictionary spam mail $ if dir then getFiles else parseFile

updateSpamTable :: Dictionary -> Mail -> FilePath -> IO ()
updateSpamTable dict m fp = do
    conn <- connectSqlite3 fp
    stmt <- prepare conn "INSERT OR REPLACE INTO spam VALUES (?, ?)"
    let spamOrHam = isSpam dict m
    executeMany stmt $ dictToSql dict
    commit conn
    disconnect conn

main :: IO ()
main = do
    args <- getArgs
    case args of
         ["init"]               -> createSpamTable dbPath
         ["debug"]              -> readDictionary dbPath >>= print
         ["build", spam, mail]  -> createDictionary spam mail >>= buildSpamTable dbPath
         ["run", fp]            -> do
              mail <- liftM Mail $ parseFile fp
              dict <- readDictionary dbPath
              print $ spamacity dict mail
         _ -> putStrLn "Usage:  spam init\n\tspam build <spam> <mail>\n\tspam run <mail>"
    where dbPath = "spam.db"

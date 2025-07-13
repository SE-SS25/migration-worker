package main

import (
	"context"
	"fmt"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"github.com/linusgith/migration-worker/src/database/postgres"
	sqlc "github.com/linusgith/migration-worker/src/database/postgres/sqlc"
	"github.com/linusgith/migration-worker/src/utils"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
	"log"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MigrationWorker struct {
	uuid        string //is assigned through an env variable by the controller
	failureMode bool
	logger      *zap.Logger
	writer      *postgres.Writer
	reader      *postgres.Reader
	writerPerf  *postgres.WriterPerfectionist
	readerPerf  *postgres.ReaderPerfectionist
}

type Document bson.Raw

type collectionStruct struct {
	number         int
	collectionName string
	documentList   []Document
}

func (mm *MigrationWorker) Heartbeat(ctx context.Context) {

	heartbeatInterval := goutils.Log().ParseEnvDurationDefault("HEARTBEAT_BACKOFF", 3*time.Second, mm.logger)

	for {

		start := time.Now()

		//heartbeatInterval is also used to increase the uptime accordingly
		err := mm.writerPerf.Heartbeat(ctx, mm.uuid, heartbeatInterval)
		if err != nil {

			mm.logger.Error("could not heartbeat, trying to remove self from table", zap.Error(err))

			//desperate attempt to remove self from the table
			removeErr := mm.writerPerf.RemoveSelf(ctx, mm.uuid)
			if removeErr != nil {
				mm.logger.Fatal("could not remove self from table, exiting with status code 1", zap.Error(err))
			}

			os.Exit(0)
		}

		mm.logger.Debug("successfully heartbeat", zap.Time("time", start))

		end := time.Now()

		timeToSleep := heartbeatInterval - start.Sub(end)

		time.Sleep(timeToSleep)
	}

}

func (mm *MigrationWorker) PrepareMigration(ctx context.Context, job sqlc.DbMigration) (*mongo.Client, *mongo.Client, error) {

	//first we need connections to the two databases for the migration

	mappings, err := mm.readerPerf.GetAllMappings(ctx)
	if err != nil {
		mm.logger.Error("error occurred getting all mappings from the database")
		return &mongo.Client{}, &mongo.Client{}, err
	}

	urlForRange, err := getOriginUrl(mappings, job, mm.logger)
	if err != nil {
		return nil, nil, err
	}

	if urlForRange == "" {
		return &mongo.Client{}, &mongo.Client{}, fmt.Errorf("there was no mapping with a matching range to be chosen as an origin for the migration")
	}

	//connect to origin db
	originCli, err := utils.SetupMongoConn(mm.logger, urlForRange)
	if err != nil {
		return &mongo.Client{}, &mongo.Client{}, err
	}

	//get current collections to create "n+1"st and add metadata collection

	//connect do destination db
	destCli, err := utils.SetupMongoConn(mm.logger, job.Url)
	if err != nil {
		return &mongo.Client{}, &mongo.Client{}, err
	}

	err = prepareDbForMigration(ctx, originCli, destCli, job, mm.logger, mm.failureMode)
	if err != nil {
		return nil, nil, fmt.Errorf("error occurred preparing the metadata for the new database: %v", err)
	}

	return originCli, destCli, nil

}

func getOriginUrl(mappings []sqlc.DbMapping, job sqlc.DbMigration, logger *zap.Logger) (string, error) {
	var urlForRange string

	for i, mp := range mappings {

		logger.Debug("comparing mapping", zap.String("mpFrom", mp.From), zap.String("jobFrom", job.From))
		//if the 'from' of the current range is smaller than or equal to the 'from' of the job range, we know, that we are on the correct origin db
		if mp.From > job.From {
			if i == 0 {
				return "", fmt.Errorf("somehow the beginning of the range that should be migrated was before 'a' -> user issue: not supported; sucks to suck")
			}
			urlForRange = mappings[i-1].Url
			break
		}

	}
	return urlForRange, nil
}

func (mm *MigrationWorker) Migrate(ctx context.Context, originCli, destCli *mongo.Client, job sqlc.DbMigration) error {

	dbNames, listErr := originCli.ListDatabaseNames(ctx, bson.D{})
	if listErr != nil {
		return listErr
	}

	//sort the names so that we can go through them and determine which databases should be migrated
	sort.Strings(dbNames)

	for _, name := range dbNames {

		//only get names of the dbs that have a 'bigger' name than the migrations 'from' and smaller than migrations 'to'
		if name >= job.From && name < job.To {
			var wg sync.WaitGroup

			if name == "admin" || name == "config" || name == "local" {
				continue
			}

			wg.Add(1)

			go func(dbName string) {
				defer wg.Done()

				mongoDbOrigin := originCli.Database(name)
				mongoDbDest := destCli.Database(name)

				collectionList, err := createCollectionStructs(ctx, mongoDbOrigin, mm.logger)
				if err != nil {
					panic(err)
				}

				for i, j := 0, len(collectionList)-1; i < j; i, j = i+1, j-1 {
					collectionList[i], collectionList[j] = collectionList[j], collectionList[i]
				}

				maximum, err := getMaxCollForDb(ctx, mongoDbDest)
				if err != nil {
					return
				}

				for _, collStruct := range collectionList {

					mm.logger.Debug("moving collection for database", zap.String("database", name), zap.String("collection", collStruct.collectionName))

					col := mongoDbOrigin.Collection(collStruct.collectionName)

					mm.logger.Debug("got origin collection", zap.String("collection", collStruct.collectionName))

					cursor, err := col.Find(ctx, &bson.D{})
					if err != nil {
						return
					}

					allCollections, err := mongoDbDest.ListCollectionNames(ctx, &bson.D{})
					if err != nil {
						mm.logger.Error("could not get collection names from destination database", zap.String("database", name), zap.Error(err))
						return
					}

					if slices.Contains(allCollections, collStruct.collectionName) && mm.failureMode {
						err = mongoDbDest.Collection(collStruct.collectionName).Drop(ctx)
						if err != nil {
							mm.logger.Error("could not drop collection in destination database", zap.String("collection", collStruct.collectionName), zap.Error(err))
							return
						}
					}

					for cursor.Next(ctx) {

						mm.logger.Debug("found document in collection", zap.String("collection", collStruct.collectionName))

						if collStruct.number > maximum {
							_, err := mongoDbDest.Collection("chat_"+strconv.Itoa(maximum)).InsertOne(ctx, append([]byte{}, cursor.Current...))
							if err != nil {
								return
							}

							mm.logger.Debug("executing special case for collection", zap.String("collection", collStruct.collectionName))

							continue
						}

						_, err := destCli.Database(name).Collection(collStruct.collectionName).InsertOne(ctx, append([]byte{}, cursor.Current...))
						if err != nil {
							return
						}

						mm.logger.Debug("inserted document successfully", zap.String("collection", collStruct.collectionName))

					}

					mm.logger.Debug("inserted all documents for collection", zap.String("collection", collStruct.collectionName), zap.Int("len", len(collStruct.documentList)))

					err = originCli.Database(name).Collection(collStruct.collectionName).Drop(ctx)
					if err != nil {
						mm.logger.Error("could not drop collection", zap.String("collection", collStruct.collectionName), zap.Error(err))
						return
					}

				}

				mm.logger.Debug("done migrating the database", zap.String("database", name))

				err = originCli.Database(name).Drop(ctx)
				if err != nil {
					return
				}

				mm.logger.Debug("successfully dropped database", zap.String("database", name))
			}(name)

			//Wait for all goroutines to finish
			wg.Wait()

			mm.logger.Debug("All goroutines are done, continuing")

		}
	}

	return nil

}

func (mm *MigrationWorker) CleanupMigration(ctx context.Context, originCli *mongo.Client, job sqlc.DbMigration) error {

	mm.logger.Debug("starting cleanup of migration", zap.String("jobId", job.ID.String()))

	dbNames, listErr := originCli.ListDatabaseNames(ctx, bson.D{})
	if listErr != nil {
		return listErr
	}

	//sort the names so that we can go through them and determine which databases should be migrated
	sort.Strings(dbNames)

	for _, name := range dbNames {

		if name >= job.From && name < job.To {

			if name == "admin" || name == "config" || name == "local" {
				continue
			}

			err := originCli.Database(name).Drop(ctx)
			if err != nil {
				return fmt.Errorf("could not drop database %s: %v", name, err)
			}

			mm.logger.Debug("successfully dropped database", zap.String("database", name))

		}

	}

	return nil

}

// createCollectionStructs gets all collections from a database and assigns every one a channel over which the data that is read from the reader of the origin database can be transferred to the writer of the destination other database.
// It then saves all channels in a sync.Map with the collection name. That map is then stored in another sync.Map that maps from the database to its collections.
func createCollectionStructs(ctx context.Context, mongoDb *mongo.Database, logger *zap.Logger) ([]collectionStruct, error) {

	collNames, err := mongoDb.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	sorted, err := sortCollectionsByName(collNames)
	if err != nil {
		return nil, err
	}

	logger.Debug("successfully sorted collections for db", zap.String("dbName", mongoDb.Name()))

	var collDocuList []collectionStruct

	for _, s := range sorted {

		numAsString, ok := strings.CutPrefix(s, "chat_")
		if !ok {
			return nil, fmt.Errorf("prefix 'chat_' not found in %s", s)
		}

		num, convErr := strconv.Atoi(numAsString)
		if convErr != nil {
			return nil, err
		}

		//'chat0' is the metadata, and we have already added it to the db
		if num == 0 {
			continue
		}

		collections := collectionStruct{
			number:         num,
			collectionName: s,
			documentList:   make([]Document, 0),
		}

		collDocuList = append(collDocuList, collections)

		logger.Debug("appended collectionStruct to list", zap.String("collName", s), zap.Int("number", num))
	}

	return collDocuList, nil

}

type infoOnDatabase struct {
	dbName            string
	highestCollection int
	metadataDoc       Document
}

// prepareDbForMigration first gets the metadata collection for every database and the highest collection and transfers these infos to the migration goal db
func prepareDbForMigration(ctx context.Context, ogClient, goalClient *mongo.Client, migration sqlc.DbMigration, logger *zap.Logger, failureMode bool) error {

	names, err := ogClient.ListDatabaseNames(ctx, &bson.D{})
	if err != nil {
		return err
	}

	logger.Debug("got all databases", zap.Int("count", len(names)))

	metadataList := make([]infoOnDatabase, 0, len(names))

	for _, name := range names {

		if name >= migration.To || name < migration.From {
			logger.Debug("database does not belong to range", zap.String("db", name), zap.String("from", migration.From), zap.String("to", migration.To))
			continue
		}

		if name == "admin" || name == "config" || name == "date" {
			continue
		}

		coll := ogClient.Database(name).Collection("chat_0")

		logger.Debug("got collection 'chat_0' for database", zap.String("db", name))

		cursor, err := coll.Find(ctx, &bson.D{})
		if err != nil {
			return err
		}

		logger.Debug("successfully got cursor for database", zap.String("db", name))

		//taking the first document out of the collection, this does NOT work if every user is one document
		ok := cursor.Next(ctx)
		if !ok {
			return fmt.Errorf("could not find metadata document with name '%s' for db %s", "chat_0", name)
		}

		metadataBson := append([]byte{}, cursor.Current...)

		logger.Debug("successfully got bytes for metadata")

		cNames, err := ogClient.Database(name).ListCollectionNames(ctx, &bson.D{})
		if err != nil {
			return err
		}

		sorted, err := sortCollectionsByName(cNames)
		if err != nil {
			return err
		}

		logger.Debug("successfully sorted the database names")

		//this theoretically should not happen because if there is a database there should always be chat0 in it even if there are no messages
		if len(sorted) == 0 {
			return fmt.Errorf("there are no collections with the right schema in this database: %s -> (specifically chat_0 is missing)", name)
		}

		highestColl, ok := strings.CutPrefix(sorted[len(sorted)-1], "chat_")
		if !ok {
			return fmt.Errorf("prefix 'chat_' not found in %s", sorted[len(sorted)-1])
		}

		maximum, err := strconv.Atoi(highestColl)
		if err != nil {
			return err
		}

		maximum = maximum + 1

		metadataList = append(metadataList, infoOnDatabase{
			dbName:            name,
			highestCollection: maximum,
			metadataDoc:       Document(metadataBson),
		})

		logger.Debug("successfully appended info to metadatList", zap.String("dbName", name), zap.Int("maximum", maximum), zap.Int("metadataLength", len(metadataBson)))

	}

	logger.Info("successfully read all metadata from origin, attempting to write to destination now")

	goalNames, err := goalClient.ListDatabaseNames(ctx, &bson.D{})
	if err != nil {
		return err
	}

	for _, elem := range metadataList {

		db := goalClient.Database(elem.dbName)

		if slices.Contains(goalNames, elem.dbName) && failureMode {

			logger.Warn("in failure mode and we have already transferred this db", zap.String("dbName", elem.dbName))

			//if db already exists, and we are in failure mode, we delete the metadata doc and write it again since we don't know if it worked last time
			dropErr := db.Collection("chat_0").Drop(ctx)
			if dropErr != nil {
				return dropErr
			}

		}

		logger.Debug("successfully got database on destination instance", zap.String("name", elem.dbName))

		var parsed bson.D

		err := bson.Unmarshal(elem.metadataDoc, &parsed)
		if err != nil {
			return fmt.Errorf("error unmarshaling the metadata into bson.D: %v", err)
		}

		logger.Debug("successfully unmarshaled metadata for database", zap.String("name", elem.dbName))

		_, err = db.Collection("chat_0").InsertOne(ctx, parsed)
		if err != nil {
			return err
		}

		logger.Debug("successfully inserted metadata document into collection chat")

		doc := bson.D{{Key: "dont_delete_me", Value: true}}

		// Step 2: Marshal to BSON bytes
		bytes, err := bson.Marshal(doc)
		if err != nil {
			log.Fatal("Failed to marshal:", err)
		}

		highestName := "chat_" + strconv.Itoa(elem.highestCollection)
		collNames, err := db.ListCollectionNames(ctx, &bson.D{})
		if err != nil {
			return err
		}

		//if the collection with the highest number does not exist, we create it
		if !slices.Contains(collNames, highestName) {

			_, err = db.Collection("chat_"+strconv.Itoa(elem.highestCollection)).InsertOne(ctx, bson.Raw(bytes))
			if err != nil {
				return err
			}
			logger.Debug("successfully inserted starting collection into destination db", zap.String("collection", highestName))
		}

		logger.Debug("successfully inserted starting collection into destination db")

	}

	return nil

}

func (mm *MigrationWorker) CalculateNewMappings(ctx context.Context, job sqlc.DbMigration) error {

	//first get all mappings from the database
	mappings, err := mm.readerPerf.GetAllMappings(ctx)
	if err != nil {
		return fmt.Errorf("could not get all db mappings: %w", err)
	}

	//set the range of the migration to where the range is now
	err = mm.writerPerf.AddDbMapping(ctx, job.From, job.Url)
	if err != nil {
		return err
	}

	//get the url of where the migration came from
	url, err := getOriginUrl(mappings, job, mm.logger)
	if err != nil {
		return fmt.Errorf("could not get origin url from mappings: %w", err)
	}

	//add the end of the migration range as the start of the new range
	err = mm.writerPerf.AddDbMapping(ctx, job.To, url)
	if err != nil {
		return fmt.Errorf("could not add db mapping for destination: %w", err)
	}

	return nil
}

func sortCollectionsByName(names []string) ([]string, error) {

	var numbers []int

	for _, s := range names {

		if s == "delete_me" {
			continue
		}

		n, ok := strings.CutPrefix(s, "chat_")
		if !ok {
			return nil, fmt.Errorf("prefix 'chat_' not found in %s", s)
		}
		number, err := strconv.Atoi(n)
		if err != nil {
			return nil, err
		}

		numbers = append(numbers, number)

	}

	sort.Ints(numbers)

	var sorted []string

	for _, n := range numbers {

		sorted = append(sorted, "chat_"+strconv.Itoa(n))
	}

	return sorted, nil

}

//func checkMessagePresent(ctx context.Context, mongoColl *mongo.Collection, doc bson.D) {
//
//	mongoColl.
//
//}

func getMaxCollForDb(ctx context.Context, mongoDb *mongo.Database) (int, error) {

	names, err := mongoDb.ListCollectionNames(ctx, &bson.D{})
	if err != nil {
		return 0, err
	}

	maximum := 0

	for _, n := range names {
		if !strings.HasPrefix(n, "chat_") {
			continue
		}

		numString, ok := strings.CutPrefix(n, "chat_")
		if !ok {
			return 0, fmt.Errorf("could not cut prefix for %s", n)
		}

		num, err := strconv.Atoi(numString)
		if err != nil {
			return 0, err
		}

		if num > maximum {
			maximum = num
		}
	}

	return maximum, err

}

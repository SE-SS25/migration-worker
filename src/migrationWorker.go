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
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MigrationWorker struct {
	uuid       string //is assigned through an env variable by the controller
	logger     *zap.Logger
	writer     *postgres.Writer
	reader     *postgres.Reader
	writerPerf *postgres.WriterPerfectionist
	readerPerf *postgres.ReaderPerfectionist
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

	var urlForRange string

	for _, mp := range mappings {

		mm.logger.Debug("comparing job from to beginning of ranges", zap.String("jobFrom", job.From), zap.String("mappingFrom", mp.From))

		//if the 'from' of the current range is smaller than or equal to the 'from' of the job range, we know, that we are on the correct origin db
		if mp.From <= job.From {
			urlForRange = mp.Url
			break
		}

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

	err = prepareDbForMigration(ctx, originCli, destCli, job)
	if err != nil {
		return nil, nil, err
	}

	return originCli, destCli, nil

}

func (mm *MigrationWorker) RunMigration(ctx context.Context, originCli, destCli *mongo.Client, job sqlc.DbMigration) error {

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

			go func() {
				wg.Add(1)
				defer wg.Done()

				mongoDbOrigin := originCli.Database(name)

				collectionList, err := createCollectionStructs(ctx, mongoDbOrigin)
				if err != nil {
					panic(err)
				}

				collections, err := readCollectionsFromDb(ctx, mongoDbOrigin, collectionList)
				if err != nil {
					panic(err)
				}

				//readPanicMsg := recover() //TODO
				//mm.logger.Error("panic occurred when running migration (reading from old)", zap.Any("msg", readPanicMsg))

				mongoDbDest := destCli.Database(name)
				err = writeCollectionsToNewDb(ctx, mongoDbDest, collections)
				if err != nil {
					panic(err)
				}

				// Wait for all goroutines to finish
				wg.Wait()

				//writePanicMsg := recover()
				//mm.logger.Error("panic occurred when running migration (writing to new)", zap.Any("msg", writePanicMsg))
			}()

		}
	}

	return nil

}

// createCollectionStructs gets all collections from a database and assigns every one a channel over which the data that is read from the reader of the origin database can be transferred to the writer of the destination other database.
// It then saves all channels in a sync.Map with the collection name. That map is then stored in another sync.Map that maps from the database to its collections.
func createCollectionStructs(ctx context.Context, mongoDb *mongo.Database) ([]collectionStruct, error) {

	collNames, err := mongoDb.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	sorted, err := sortCollectionsByName(collNames)
	if err != nil {
		return nil, err
	}

	var collDocuList []collectionStruct

	for _, s := range sorted {

		numAsString, ok := strings.CutPrefix(s, "chat")
		if !ok {
			return nil, fmt.Errorf("prefix 'chat' not found in %s", s)
		}

		num, convErr := strconv.Atoi(numAsString)
		if convErr != nil {
			return nil, err
		}

		collections := collectionStruct{
			number:         num,
			collectionName: s,
			documentList:   make([]Document, 0),
		}

		collDocuList = append(collDocuList, collections)

	}

	return collDocuList, nil

}

// readCollectionsFromDb starts a goroutine for every collection and writes the data from that collection into the corresponding channel
func readCollectionsFromDb(ctx context.Context, mongoDb *mongo.Database, collList []collectionStruct) ([]collectionStruct, error) {

	for _, collection := range collList {

		col := mongoDb.Collection(collection.collectionName)

		cursor, err := col.Find(ctx, &bson.D{})
		if err != nil {
			return nil, err
		}

		for cursor.Next(ctx) {
			collection.documentList = append(collection.documentList, Document(cursor.Current))
		}

	}

	return collList, nil

}

func writeCollectionsToNewDb(ctx context.Context, mongoDb *mongo.Database, collList []collectionStruct) error {

	//this reverses the list
	for i, j := 0, len(collList)-1; i < j; i, j = i+1, j-1 {
		collList[i], collList[j] = collList[j], collList[i]
	}

	//first write the metadata
	metadataDoc := collList[len(collList)-1]

	metadataColl := mongoDb.Collection(metadataDoc.collectionName)
	_, err := metadataColl.InsertOne(ctx, bson.Raw(metadataDoc.documentList[0]))
	if err != nil {
		return err
	}

	collList = collList[0 : len(collList)-1]

	for _, collection := range collList {

		coll := mongoDb.Collection(collection.collectionName)

		_, err = coll.InsertMany(ctx, collection.documentList)
		if err != nil {
			return err
		}

	}

	return nil
}

type infoOnDatabase struct {
	dbName            string
	highestCollection int
	metadataDoc       Document
}

func prepareDbForMigration(ctx context.Context, ogClient, goalClient *mongo.Client, migration sqlc.DbMigration) error {

	names, err := ogClient.ListDatabaseNames(ctx, &bson.D{})
	if err != nil {
		return err
	}

	metadataList := make([]infoOnDatabase, len(names))

	for _, name := range names {

		if name >= migration.To && name < migration.From {
			continue
		}

		coll := ogClient.Database(name).Collection("chat0")

		cursor, err := coll.Find(ctx, &bson.D{})
		if err != nil {
			return err
		}

		ok := cursor.Next(ctx)
		if !ok {
			fmt.Errorf("could not find metadata document")
		}

		metadataBson := cursor.Current

		cNames, err := ogClient.Database(name).ListCollectionNames(ctx, &bson.D{})
		if err != nil {
			return err
		}

		sorted, err := sortCollectionsByName(cNames)
		if err != nil {
			return err
		}

		highestColl, ok := strings.CutPrefix(sorted[len(sorted)-1], "chat")
		if !ok {
			return fmt.Errorf("prefix 'chat' not found in %s", sorted[len(sorted)-1])
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

	}

	for _, elem := range metadataList {

		db := goalClient.Database(elem.dbName)
		_, err := db.Collection("chat0").InsertOne(ctx, elem.metadataDoc)
		if err != nil {
			return err
		}
		_, err = db.Collection("chat"+strconv.Itoa(elem.highestCollection)).InsertOne(ctx, bson.Raw("delete_me"))
		if err != nil {
			return err
		}
	}

	return nil

}

func sortCollectionsByName(names []string) ([]string, error) {

	var numbers []int

	for _, s := range names {

		if s == "admin" || s == "config" || s == "date" {
			continue
		}

		n, ok := strings.CutPrefix(s, "chat")
		if !ok {
			return nil, fmt.Errorf("prefix 'chat' not found in %s", s)
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

		sorted = append(sorted, "chat"+strconv.Itoa(n))
	}

	return sorted, nil

}

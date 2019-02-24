package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Post struct {
	ID        primitive.ObjectID `bson:"_id"`
	Title     string             `bson:"title"`
	Body      string             `bson:"body"`
	Tags      []string           `bson:"tags"`
	Comments  uint64             `bson:"comments"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt *time.Time         `bson:"updated_at"`
}

func main() {
	// Initialising and connecting
	// ========================================================================================

	// create a new timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// create a mongo client
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:6548/"))
	if err != nil {
		log.Fatal(err)
	}

	// disconnect from mongo
	defer client.Disconnect(ctx)

	// select collection from database
	col := client.Database("blog").Collection("posts")

	// InsertOne
	// ========================================================================================

	{
		res, err := col.InsertOne(ctx, bson.M{
			"title": "Go mongodb driver cookbook",
			"tags":  []string{"golang", "mongodb"},
			"body": `this is a long post
	that goes on and on
	and have many lines`,
			"comments":   1,
			"created_at": time.Now(),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("inserted id: %s\n", res.InsertedID.(primitive.ObjectID).Hex())
		// => inserted id: 5c71caf32a346553363177ce
	}

	// InsertMany
	// ========================================================================================

	{
		res, err := col.InsertMany(ctx, []interface{}{
			bson.M{
				"title":      "Post one",
				"tags":       []string{"golang"},
				"body":       "post one body",
				"comments":   1,
				"created_at": time.Date(2019, time.January, 10, 15, 30, 0, 0, time.UTC),
			},
			bson.M{
				"title":      "Post two",
				"tags":       []string{"nodejs"},
				"body":       "post two body",
				"comments":   1,
				"created_at": time.Now(),
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("inserted ids: %v\n", res.InsertedIDs)
		// => inserted ids: [ObjectID("5c71ce5c6e6d43eb6e2e93be") ObjectID("5c71ce5c6e6d43eb6e2e93bf")]
	}

	// UpdateOne
	// ========================================================================================

	{
		// create ObjectID from string
		id, err := primitive.ObjectIDFromHex("5c71ce5c6e6d43eb6e2e93be")
		if err != nil {
			log.Fatal(err)
		}

		// set filters and updates
		filter := bson.M{"_id": id}
		update := bson.M{"$set": bson.M{"title": "post 2 (two)"}}

		// update document
		res, err := col.UpdateOne(ctx, filter, update)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("modified count: %d\n", res.ModifiedCount)
		// => modified count: 1
	}

	// UpdateMany
	// ========================================================================================

	{
		// set filters and updates
		filter := bson.M{"tags": bson.M{"$elemMatch": bson.M{"$eq": "golang"}}}
		update := bson.M{"$set": bson.M{"comments": 0, "updated_at": time.Now()}}

		// update documents
		res, err := col.UpdateMany(ctx, filter, update, options.Update().SetUpsert(true))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("modified count: %d\n", res.ModifiedCount)
		// => modified count: 17
	}

	// DeleteOne
	// ========================================================================================

	{
		// create ObjectID from string
		id, err := primitive.ObjectIDFromHex("5c71ce5c6e6d43eb6e2e93be")
		if err != nil {
			log.Fatal(err)
		}

		// delete document
		res, err := col.DeleteOne(ctx, bson.M{"_id": id})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("deleted count: %d\n", res.DeletedCount)
		// => deleted count: 1
	}

	// DeleteMany
	// ========================================================================================

	{
		// delete documents created older than 2 days
		filter := bson.M{"created_at": bson.M{
			"$lt": time.Now().Add(-2 * 24 * time.Hour),
		}}

		// update documents
		res, err := col.DeleteMany(ctx, filter)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("deleted count: %d\n", res.DeletedCount)
		// => deleted count: 7
	}

	// FindOne
	// ========================================================================================

	{

		// filter posts tagged as golang
		filter := bson.M{"tags": bson.M{"$elemMatch": bson.M{"$eq": "golang"}}}

		// find one document
		var p Post
		if col.FindOne(ctx, filter).Decode(&p); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("post: %+v\n", p)
	}

	// Find
	// ========================================================================================

	{
		// filter posts tagged as golang
		filter := bson.M{"tags": bson.M{"$elemMatch": bson.M{"$eq": "golang"}}}

		// find all documents
		cursor, err := col.Find(ctx, filter)
		if err != nil {
			log.Fatal(err)
		}

		// iterate through all documents
		for cursor.Next(ctx) {
			var p Post
			// decode the document
			if err := cursor.Decode(&p); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("post: %+v\n", p)
		}

		// check if the cursor encountered any errors while iterating
		if err := cursor.Err(); err != nil {
			log.Fatal(err)
		}
	}

	// BulkWrite
	// ========================================================================================

	{
		// list of inserts
		inserts := []bson.M{
			{
				"title":      "post five",
				"tags":       []string{"postgresql"},
				"created_at": time.Now(),
			},
			{
				"title":      "post six",
				"tags":       []string{"graphql"},
				"created_at": time.Now(),
			},
		}

		// list of updates
		updates := []struct {
			filter  bson.M
			updates bson.M
		}{
			{
				filter: bson.M{
					"tags": bson.M{"$elemMatch": bson.M{"$eq": "golang"}},
				},
				updates: bson.M{"$set": bson.M{"updated_at": time.Now()}},
			},
		}

		// list of deletes
		id1, _ := primitive.ObjectIDFromHex("5c71ce5c6e6d43eb6e2e93be") // don't do this, handle the error
		id2, _ := primitive.ObjectIDFromHex("5c727424f8eaee29d1d1e7eb")
		id3, _ := primitive.ObjectIDFromHex("5c727424f8eaee29d1d1e7ea")
		deletes := []bson.M{
			{"_id": id1},
			{"_id": id2},
			{"_id": id3},
		}

		// range over each list of operations and create the write model
		var writes []mongo.WriteModel
		for _, ins := range inserts {
			model := mongo.NewInsertOneModel().SetDocument(ins)
			writes = append(writes, model)
		}
		for _, upd := range updates {
			model := mongo.NewUpdateManyModel().
				SetFilter(upd.filter).
				SetUpdate(upd.updates)
			writes = append(writes, model)
		}
		for _, del := range deletes {
			model := mongo.NewDeleteManyModel().SetFilter(del)
			writes = append(writes, model)
		}

		// run bulk write
		res, err := col.BulkWrite(ctx, writes)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf(
			"insert: %d, updated: %d, deleted: %d",
			res.InsertedCount,
			res.ModifiedCount,
			res.DeletedCount,
		)
		// => insert: 2, updated: 10, deleted: 3
	}
}

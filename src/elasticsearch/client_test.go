package elasticsearch_client

import (
	"context"
	"fmt"
	"testing"
)

func TestLocalElasticSearchClient(t *testing.T) {

	// host := "https://carvanalistingsnapshot-dev.es.westus2.azure.elastic-cloud.com:9243/"
	host := "http://0.0.0.0:9200"
	username := "elastic"
	password := "pass"
	index := "listing"
	es := NewElasticSearchClient(host, username, password, index)
	if err := es.CreateIndex(context.Background(), index); err != nil {
		t.Logf(err.Error())
	}

	info, err := es.GetInfo(context.Background())

	if err != nil {
		t.Errorf("Error getting info: %v", err)
	}
	fmt.Println(info)
	t.Run("Test create and get document", func(t *testing.T) {

		es.DeleteDocument(context.Background(), "1")

		doc := document{
			ID: "1",
			Source: map[string]interface{}{
				"make": "toyota",
			},
		}

		err := es.CreateDocument(context.Background(), doc.ID, &doc)

		if err != nil {
			t.Errorf("Error creating document: %v", err)
		}
		savedDoc, err := es.GetDocumentByID(context.Background(), doc.ID)

		if err != nil {
			t.Errorf("Error getting document: %v", err)
		}

		if savedDoc.Source["make"] != doc.Source["make"] {
			t.Errorf("Expected %v, got %v", doc.Source["make"], savedDoc.Source["make"])
		}
	})

	t.Run("Test create and search document by query", func(t *testing.T) {

		_ = es.DeleteDocument(context.Background(), "1")

		doc := document{
			ID: "1",
			Source: map[string]interface{}{
				"make": "toyota",
			},
		}

		err := es.CreateDocument(context.Background(), doc.ID, &doc)

		if err != nil {
			t.Errorf("Error creating document: %v", err)
		}
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		}
		savedDocs, err := es.GetDocumentsByQuery(context.Background(), query, 10, "true")

		if err != nil {
			t.Errorf("Error getting document: %v", err)
		}

		if (*savedDocs)[0].Source["make"] != doc.Source["make"] {
			t.Errorf("Expected %v, got %v", doc.Source["make"], (*savedDocs)[0].Source["make"])
		}
	})

}

func TestElasticSearchClient(t *testing.T) {

	host := "https://carvanalistingsnapshot-dev.es.westus2.azure.elastic-cloud.com:9243/"
	username := "jwu"
	password := "YC6VT9U*m983Wo"
	index := "listing"
	es := NewElasticSearchClient(host, username, password, index)

	t.Run("Test get multiple documents", func(t *testing.T) {
		size := 20000
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		}
		savedDocs, err := es.GetDocumentsByQuery(context.Background(), query, size, "false")

		if err != nil {
			t.Errorf("Error getting document: %v", err)
		}

		if len(*savedDocs) < size {
			t.Errorf("Expected %v at least, got %v", size, len(*savedDocs))
		}
	})

}

package elasticsearch_client

import (
	"context"
	"fmt"
	"testing"
)

func TestLocalElasticSearchClient(t *testing.T) {

	host := "http://127.0.0.1:9200"
	username := "elastic"
	password := "pass"
	index := "listing"
	es := NewClient(host, username, password, index)
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

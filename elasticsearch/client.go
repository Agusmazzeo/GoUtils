package elasticsearch_client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Agusmazzeo/GoUtils/utils"

	"github.com/elastic/go-elasticsearch/v8"
)

type document struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

func (d *document) Data() map[string]interface{} {
	return d.Source
}

type searchResult struct {
	Took     int64  `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   shards `json:"_shards"`
	Hits     hits   `json:"hits"`
	ScrollID string `json:"_scroll_id"`
}

type shards struct {
	Total      int64 `json:"total"`
	Successful int64 `json:"successful"`
	Skipped    int64 `json:"skipped"`
	Failed     int64 `json:"failed"`
}

type hits struct {
	Total    total      `json:"total"`
	MaxScore float64    `json:"max_score"`
	Hits     []document `json:"hits"`
}

type total struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

type ElasticSearchClient struct {
	client  *elasticsearch.Client
	index   string
	timeout time.Duration
}

func NewElasticSearchClient(host, username, password, index string) *ElasticSearchClient {
	cfg := elasticsearch.Config{
		Addresses: []string{host},
		Username:  username,
		Password:  password,
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	return &ElasticSearchClient{client: client, index: index}
}

func (c *ElasticSearchClient) CreateIndex(ctx context.Context, index string) error {
	res, err := c.client.Indices.Create(index)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("create index error: response: %s", res.String())
	}
	return nil
}

func (c *ElasticSearchClient) GetInfo(ctx context.Context) (string, error) {
	res, err := c.client.Info()
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	return res.String(), nil
}

func (c *ElasticSearchClient) GetDocumentByID(ctx context.Context, id string) (*document, error) {
	doc := document{}

	res, err := c.client.Get(c.index, id)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		panic("Document not found")
	}

	err = json.NewDecoder(res.Body).Decode(&doc)
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

func (c *ElasticSearchClient) GetDocumentsByQuery(ctx context.Context, query interface{}, size int, sourceInclude string) (*[]document, error) {
	documentIDs := []document{}
	result := searchResult{}

	queryJson, err := utils.MappingToString(query)
	if err != nil {
		return nil, err
	}

	querySize := size
	if size > 10000 {
		querySize = 10000
	}
	res, err := c.client.Search(
		c.client.Search.WithIndex(c.index),
		c.client.Search.WithBody(strings.NewReader((queryJson))),
		c.client.Search.WithSource(sourceInclude),
		c.client.Search.WithSize(querySize),
		c.client.Search.WithTrackTotalHits(true),
		c.client.Search.WithScroll(time.Minute),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("search error: response: %s", res.String())
	}

	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	documentIDs = append(documentIDs, result.Hits.Hits...)
	scrollID := result.ScrollID
	hitCount := len(result.Hits.Hits)
	for {
		if (hitCount < querySize) || (len(documentIDs) >= size) {
			break
		}
		res, err := c.GetDocumentIDsFromScroll(ctx, scrollID)
		if err != nil {
			return nil, err
		}
		scrollID = res.ScrollID
		hitCount = len(res.Hits.Hits)
		documentIDs = append(documentIDs, res.Hits.Hits...)
	}

	return &documentIDs, nil
}

func (c *ElasticSearchClient) GetDocumentIDsFromScroll(ctx context.Context, scrollID string) (*searchResult, error) {

	result := searchResult{}

	res, err := c.client.Scroll(
		c.client.Scroll.WithScrollID(scrollID),
		c.client.Scroll.WithScroll(time.Minute),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *ElasticSearchClient) CreateDocument(ctx context.Context, id string, doc *document) error {

	docJson, err := json.Marshal(doc.Data())
	if err != nil {
		return err
	}

	docByte := bytes.NewReader(docJson)
	res, err := c.client.Create(c.index, id, docByte)

	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 409 {
		return fmt.Errorf("inserted document already exists: %s", res.String())
	}

	if res.IsError() {
		return fmt.Errorf("insert error: response: %s", res.String())
	}
	return nil
}

func (c *ElasticSearchClient) DeleteDocument(ctx context.Context, id string) error {

	res, err := c.client.Delete(c.index, id)

	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("insert error: response: %s", res.String())
	}
	return nil
}

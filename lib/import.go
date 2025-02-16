package lib

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/nbd-wtf/go-nostr"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Workers

func ImportEvents() {

	data, err := os.ReadFile("./export.json")
	if err != nil {
		panic(err)
	}

	events := make(chan nostr.Event)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		ParseEvents(events)
	}()

	var event nostr.Event

	for i, line := range strings.Split(string(data), "\n") {
		if i > 10000 {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		event = nostr.Event{}
		err = json.Unmarshal([]byte(line), &event)
		if err != nil {
			log.Println("Invalid event:", event)
		}

		events <- event
	}

	close(events)
	wg.Wait()
}

func ParseEvents(events chan nostr.Event) {
	subgraphChannel := make(chan Subgraph)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		MergeEntities(subgraphChannel)
	}()

	for event := range events {
		// fmt.Println(event.ID)
		subgraph := *NewSubgraph()

		// Create User and Event nodes
		userNode := NewUserNode(event.PubKey)
		eventNode := NewEventNode(event.ID)

		eventNode.Props["created_at"] = event.CreatedAt.Time().Unix()
		eventNode.Props["kind"] = event.Kind
		eventNode.Props["content"] = event.Content

		authorRel := NewSignedRel(userNode, eventNode, nil)

		subgraph.AddNode(userNode)
		subgraph.AddNode(eventNode)
		subgraph.AddRel(authorRel)

		// Create Tag nodes
		for _, tag := range event.Tags {
			if len(tag) >= 2 {
				name := tag[0]
				value := tag[1]

				// Special cases

				tagNode := NewTagNode(name, value)
				tagRel := NewTaggedRel(eventNode, tagNode, nil)
				subgraph.AddNode(tagNode)
				subgraph.AddRel(tagRel)
			}
		}

		subgraphChannel <- subgraph
	}

	close(subgraphChannel)
	wg.Wait()
}

func MergeEntities(subgraphChannel chan Subgraph) {
	ctx := context.Background()
	driver, err := connectNeo4j(ctx)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)

	batchSize := 25000
	matchProvider := NewMatchKeys()
	subgraph := NewStructuredSubgraph(matchProvider)

	for sg := range subgraphChannel {
		for _, node := range sg.nodes {
			subgraph.AddNode(node)
		}
		for _, rel := range sg.rels {
			subgraph.AddRel(rel)
		}

		if subgraph.NodeCount() > batchSize {
			mergeSubgraph(ctx, driver, subgraph)
			subgraph = NewStructuredSubgraph(matchProvider)
		}
	}

	mergeSubgraph(ctx, driver, subgraph)
}

// Helper Functions

func connectNeo4j(ctx context.Context) (neo4j.DriverWithContext, error) {
	dbUri := "neo4j://localhost:7687"
	dbUser := "neo4j"
	dbPassword := "neo4jnostr"
	driver, err := neo4j.NewDriverWithContext(
		dbUri,
		neo4j.BasicAuth(dbUser, dbPassword, ""))

	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		return driver, err
	}

	indexQueries := []string{
		`CREATE CONSTRAINT user_pubkey IF NOT EXISTS
		 FOR (n:User) REQUIRE n.pubkey IS UNIQUE`,

		`CREATE INDEX user_pubkey IF NOT EXISTS
		 FOR (n:User) ON (n.pubkey)`,

		`CREATE INDEX event_id IF NOT EXISTS
		 FOR (n:Event) ON (n.id)`,

		`CREATE INDEX event_kind IF NOT EXISTS
		 FOR (n:Event) ON (n.kind)`,

		`CREATE INDEX tag_name_value IF NOT EXISTS
		 FOR (n:Tag) ON (n.name, n.value)`,
	}

	// Create indexes/constraints
	for _, query := range indexQueries {
		_, err = neo4j.ExecuteQuery(ctx, driver,
			query,
			nil,
			neo4j.EagerResultTransformer,
			neo4j.ExecuteQueryWithDatabase("neo4j"))

		if err != nil {
			panic(err)
		}
	}

	return driver, nil
}

func mergeSubgraph(
	ctx context.Context,
	driver neo4j.DriverWithContext,
	subgraph *StructuredSubgraph,
) {

	// fmt.Println("Got node keys:", subgraph.NodeKeys())
	// fmt.Println("Got rel keys:", subgraph.RelKeys())
	// fmt.Println("Node count:", subgraph.NodeCount())
	// fmt.Println("Rel count:", subgraph.RelCount())

	for _, nodeKey := range subgraph.NodeKeys() {
		matchLabel, labels := DeserializeNodeKey(nodeKey)
		mergeNodes(
			ctx, driver,
			matchLabel,
			labels,
			subgraph.matchProvider,
			subgraph.GetNodes(nodeKey),
		)
	}

	for _, relKey := range subgraph.RelKeys() {
		rtype, startLabel, endLabel := DeserializeRelKey(relKey)
		mergeRels(
			ctx, driver,
			rtype,
			startLabel,
			endLabel,
			subgraph.matchProvider,
			subgraph.GetRels(relKey),
		)
	}
}

func mergeNodes(
	ctx context.Context,
	driver neo4j.DriverWithContext,
	matchLabel string,
	nodeLabels []string,
	matchProvider MatchKeysProvider,
	nodes []*Node,
) {
	cypherLabels := ToCypherLabels(nodeLabels)

	matchKeys, exists := matchProvider.GetKeys(matchLabel)
	if !exists {
		panic(fmt.Errorf("unknown match label: %s", matchLabel))
	}

	cypherProps := ToCypherProps(matchKeys, "node.")

	serializedNodes := []*SerializedNode{}
	for _, node := range nodes {
		serializedNodes = append(serializedNodes, node.Serialize())
	}

	query := fmt.Sprintf(`
		UNWIND $nodes as node

		MERGE (n%s { %s })
		SET n += node
		`,
		cypherLabels, cypherProps,
	)

	// fmt.Println("First node:", *serializedNodes[0])
	// fmt.Printf("Generated query:\n```\n%s\n```\n", query)

	result, err := neo4j.ExecuteQuery(ctx, driver,
		query,
		map[string]any{
			"nodes": serializedNodes,
		}, neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		panic(err)
	}

	summary := result.Summary
	fmt.Printf("Created %v nodes in %+v.\n",
		summary.Counters().NodesCreated(),
		summary.ResultAvailableAfter())
}

func mergeRels(
	ctx context.Context,
	driver neo4j.DriverWithContext,
	rtype string,
	startLabel string,
	endLabel string,
	matchProvider MatchKeysProvider,
	rels []*Relationship,
) {
	cypherType := ToCypherLabel(rtype)
	startCypherLabel := ToCypherLabel(startLabel)
	endCypherLabel := ToCypherLabel(endLabel)

	matchKeys, exists := matchProvider.GetKeys(startLabel)
	if !exists {
		panic(fmt.Errorf("unknown start node label: %s", startLabel))
	}

	startCypherProps := ToCypherProps(matchKeys, "rel.start.")

	matchKeys, exists = matchProvider.GetKeys(endLabel)
	if !exists {
		panic(fmt.Errorf("unknown end node label: %s", endLabel))
	}

	endCypherProps := ToCypherProps(matchKeys, "rel.end.")

	serializedRels := []*SerializedRel{}
	for _, rel := range rels {
		serializedRels = append(serializedRels, rel.Serialize())
	}

	query := fmt.Sprintf(`
		UNWIND $rels as rel

		MATCH (start%s { %s })
		MATCH (end%s { %s })

		CREATE (start)-[r%s]->(end)
		SET r += rel.props
		`,
		startCypherLabel, startCypherProps,
		endCypherLabel, endCypherProps,
		cypherType,
	)

	// fmt.Println("First rel:", *serializedRels[0])
	// fmt.Printf("Generated query:\n```\n%s\n```\n", query)

	result, err := neo4j.ExecuteQuery(ctx, driver,
		query,
		map[string]any{
			"rels": serializedRels,
		}, neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		panic(err)
	}

	summary := result.Summary
	fmt.Printf("Created %v relationships in %+v.\n",
		summary.Counters().RelationshipsCreated(),
		summary.ResultAvailableAfter())
}

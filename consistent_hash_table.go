package main

import (
	"errors"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
)

type ServerRingNode struct {
	serverIndex int
	posDegree   float64
}

type ConsistentHashTable struct {
	servers         []map[int]interface{}
	serverRingNodes []ServerRingNode
}

func createConsistentHashTable() ConsistentHashTable {
	var servers []map[int]interface{}
	var serverRingNodes []ServerRingNode

	m0 := map[int]interface{}{}
	for _, element := range getNodesOnRing(10) {
		serverRingNodes = append(serverRingNodes, ServerRingNode{
			serverIndex: 0,
			posDegree:   element,
		})
	}

	m1 := map[int]interface{}{}
	for _, element := range getNodesOnRing(10) {
		serverRingNodes = append(serverRingNodes, ServerRingNode{
			serverIndex: 1,
			posDegree:   element,
		})
	}

	m2 := map[int]interface{}{}
	for _, element := range getNodesOnRing(10) {
		serverRingNodes = append(serverRingNodes, ServerRingNode{
			serverIndex: 2,
			posDegree:   element,
		})
	}

	m3 := map[int]interface{}{}
	for _, element := range getNodesOnRing(10) {
		serverRingNodes = append(serverRingNodes, ServerRingNode{
			serverIndex: 3,
			posDegree:   element,
		})
	}

	servers = append(servers, m0, m1, m2, m3)
	sort.Slice(serverRingNodes, func(i, j int) bool {
		return serverRingNodes[i].posDegree < serverRingNodes[j].posDegree
	})

	return ConsistentHashTable{
		servers:         servers,
		serverRingNodes: serverRingNodes,
	}
}

func getNodesOnRing(numNodes int) []float64 {

	var res []float64

	for i := 0; i < numNodes; i++ {
		res = append(res, math.Round(rand.Float64()*360))
	}

	return res
}

func (c ConsistentHashTable) addEntry(key string, val interface{}) error {
	hashVal, hashErr := toHashVal(key)
	if hashErr != nil {
		return hashErr
	}

	ringPos := hashVal % 360
	serverLoc, locErr := findServerLoc(ringPos, c.serverRingNodes)
	if locErr != nil {
		return locErr
	}

	c.servers[serverLoc][hashVal] = val

	return nil
}

func (c ConsistentHashTable) getEntry(key string) (interface{}, error) {
	hashVal, hashErr := toHashVal(key)
	if hashErr != nil {
		return nil, hashErr
	}

	ringPos := hashVal % 360
	serverLoc, locErr := findServerLoc(ringPos, c.serverRingNodes)
	if locErr != nil {
		return nil, locErr
	}

	return c.servers[serverLoc][hashVal], nil
}

func (c ConsistentHashTable) removeEntry(key string) error {
	hashVal, hashErr := toHashVal(key)
	if hashErr != nil {
		return hashErr
	}

	ringPos := hashVal % 360
	serverLoc, locErr := findServerLoc(ringPos, c.serverRingNodes)
	if locErr != nil {
		return locErr
	}

	delete(c.servers[serverLoc], hashVal)

	return nil
}

func toHashVal(key string) (int, error) {
	hash := fnv.New32()
	_, hashErr := hash.Write([]byte(key))
	if hashErr != nil {
		return -1, hashErr
	}
	return int(hash.Sum32()), nil
}

func findServerLoc(ringPos int, serverRingNodes []ServerRingNode) (int, error) {
	for i := 0; i < len(serverRingNodes); i++ {
		if i+1 == len(serverRingNodes) || serverRingNodes[i].posDegree > float64(ringPos) {
			return serverRingNodes[i].serverIndex, nil
		}
	}

	return -1, errors.New("expected to find server location")
}

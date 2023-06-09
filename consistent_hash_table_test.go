package main

import (
	"fmt"
	"testing"
)

const TEST_KEY_1 = "TEST_1"
const TEST_VAL_1 = 2
const TEST_KEY_2 = "TEST_2"
const TEST_VAL_2 = 4

func TestInsertAndFetch(t *testing.T) {
	table := createConsistentHashTable()
	table.addEntry(TEST_KEY_1, TEST_VAL_1)

	numEntries := 0
	for i := 0; i < len(table.servers); i++ {
		numEntries += len(table.servers[i])
	}

	if numEntries != 1 {
		t.Fatal("Expected one entry")
	}

	res, _ := table.getEntry(TEST_KEY_1)
	if res != TEST_VAL_1 {
		t.Fatal(fmt.Sprintf(`expected %s to have value of %d but had %d`, TEST_KEY_1, TEST_VAL_1, res))
	}
}

func TestMultipleInsertAndFetch(t *testing.T) {
	table := createConsistentHashTable()
	table.addEntry(TEST_KEY_1, TEST_VAL_1)
	table.addEntry(TEST_KEY_2, TEST_VAL_2)

	numEntries := 0
	for i := 0; i < len(table.servers); i++ {
		numEntries += len(table.servers[i])
	}
	fmt.Print(table.servers)

	if numEntries != 2 {
		t.Fatal(fmt.Sprintf("Expected two entries but found %d", numEntries))
	}

	res, _ := table.getEntry(TEST_KEY_1)
	if res != TEST_VAL_1 {
		t.Fatal(fmt.Sprintf(`expected %s to have value of %d but had %d`, TEST_KEY_1, TEST_VAL_1, res))
	}

	res, _ = table.getEntry(TEST_KEY_2)
	if res != TEST_VAL_2 {
		t.Fatal(fmt.Sprintf(`expected %s to have value of %d but had %d`, TEST_KEY_2, TEST_VAL_2, res))
	}
}

func TestEmptyFetch(t *testing.T) {
	table := createConsistentHashTable()

	numEntries := 0
	for i := 0; i < len(table.servers); i++ {
		numEntries += len(table.servers[i])
	}

	if numEntries != 0 {
		t.Fatal("Expected no entries")
	}

	res, _ := table.getEntry(TEST_KEY_1)
	if res != nil {
		t.Fatal(fmt.Sprintf(`expected %s to no value but had %d`, TEST_KEY_1, res))
	}
}

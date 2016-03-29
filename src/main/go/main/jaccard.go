package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var edges map[string]map[string]bool
var degree map[string]int
var pairs map[string]int

func add(m map[string]map[string]bool, n1, n2 string) bool {
	mm, ok := m[n2]
	if !ok {
		mm = make(map[string]bool)
		m[n2] = mm
	}

	if !mm[n1] {
		mm[n1] = true
		return true
	}

	return false
}

func main() {

	edges = make(map[string]map[string]bool)
	degree = make(map[string]int)
	pairs = make(map[string]int)

	for _, path := range os.Args[1:] {
		_, filename := filepath.Split(path)
		file, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		org := strings.TrimSuffix(filename, ".txt")
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "::::") {
				continue
			}

			fields := strings.Split(line, ",")

			proj := org + "/" + fields[0]
			if add(edges, proj, fields[1]) {
				degree[proj]++
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		file.Close()
	}

	for _, mm := range edges {
		for n11 := range mm {
			for n12 := range mm {
				if strings.Compare(n11, n12) < 0 {
					pairs[n11+":"+n12]++
				}
			}
		}
	}

	for pair, i := range pairs {
		nodes := strings.Split(pair, ":")
		d1 := degree[nodes[0]]
		d2 := degree[nodes[1]]
		jaccard := float64(i) / float64(d1+d2-i)

		org1 := strings.Split(nodes[0], "/")[0]
		org2 := strings.Split(nodes[1], "/")[0]

		var sameorg string
		if org1 != org2 {
			sameorg = "DO"
		} else {
			sameorg = "SO"
		}
		fmt.Printf("%f %s %s %d %d %d\n", jaccard, sameorg, pair, i, d1, d2)
	}
}

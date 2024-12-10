package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/dimchansky/utfbom"
)

type Node struct {
	Headers []string
	Data    <-chan []string
}

func uniqStrs(strs []string) []string {
	var out []string
	for _, s := range strs {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

func (n1 Node) Shuffle(leadingColumns []string) (n2 Node, err error) {
	return n1.Cut(uniqStrs(append(leadingColumns, n1.Headers...)))
}

func (n1 Node) Cut(cols []string) (n2 Node, err error) {
	cols = uniqStrs(cols)
	for _, h := range cols {
		if slices.Index(n1.Headers, h) < 0 {
			return n2, fmt.Errorf("column missing from input data: '%s'", h)
		}
	}
	n2.Headers = append([]string(nil), cols...)

	edits := make([]int, len(n1.Headers))
	for i, h := range n1.Headers {
		edits[i] = slices.Index(cols, h)
	}

	ch := make(chan []string, 100)
	go func() {
		defer close(ch)
		for row := range n1.Data {
			newRow := make([]string, len(cols))
			for i, s := range row {
				if edit := edits[i]; edit >= 0 {
					newRow[edit] = s
				}
			}
			ch <- newRow
		}
	}()
	n2.Data = ch

	return n2, nil
}

func (n1 Node) Grep(col string, search string) (n2 Node, err error) {
	n2.Headers = n1.Headers

	idx := -1
	for i, h := range n1.Headers {
		if h == col {
			idx = i
			break
		}
	}
	if idx < 0 {
		return n2, fmt.Errorf("column missing from input data: '%s'", col)
	}

	ch := make(chan []string, 100)
	go func() {
		defer close(ch)

		for row := range n1.Data {
			if strings.Contains(row[idx], search) {
				ch <- row
			}
		}
	}()
	n2.Data = ch

	return n2, nil
}

func (n1 Node) Grepv(col string, search string) (n2 Node, err error) {
	n2.Headers = n1.Headers

	idx := -1
	for i, h := range n1.Headers {
		if h == col {
			idx = i
			break
		}
	}
	if idx < 0 {
		return n2, fmt.Errorf("column missing from input data: '%s'", col)
	}

	ch := make(chan []string, 100)
	go func() {
		defer close(ch)

		for row := range n1.Data {
			if !strings.Contains(row[idx], search) {
				ch <- row
			}
		}
	}()
	n2.Data = ch

	return n2, nil
}

func (n1 Node) sort(fn func(n1 []string, n2 []string) int) (n2 Node, err error) {
	n2.Headers = n1.Headers

	ch := make(chan []string, 100)
	go func() {
		defer close(ch)

		var data [][]string
		for row := range n1.Data {
			data = append(data, row)
		}
		slices.SortStableFunc(data, fn)
		for _, row := range data {
			ch <- row
		}
	}()
	n2.Data = ch

	return n2, nil
}

func (n1 Node) Join(fname string, matches [][2]string) (n2 Node, err error) {
	f, err := os.Open(fname)
	if err != nil {
		return
	}
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return
	}
	tgtHeaders := records[0]
	matchIndexes := make([][2]int, len(matches))
	tgtMapping := make([]int, len(tgtHeaders))
	n2.Headers = make([]string, len(n1.Headers)+len(tgtHeaders)-len(matches))
	copy(n2.Headers, n1.Headers)

	{
		joinHeaders := make([]string, len(matches))
		for i, m := range matches {
			joinHeaders[i] = m[1]
			matchIndexes[i][0] = slices.Index(n1.Headers, m[0])
			matchIndexes[i][1] = slices.Index(tgtHeaders, m[1])
		}
		j := len(n1.Headers)
		for i, tgtHeader := range tgtHeaders {
			if slices.Contains(joinHeaders, tgtHeader) {
				tgtMapping[i] = -1
			} else {
				tgtMapping[i] = j
				n2.Headers[j] = tgtHeader
				j++
			}
		}
	}

	tgtRecords := records[1:]

	ch := make(chan []string, 100)
	go func() {
		defer close(ch)

		for r1 := range n1.Data {
			for _, r2 := range tgtRecords {
				match := true
				for _, pair := range matchIndexes {
					if r1[pair[0]] != r2[pair[1]] {
						match = false
						break
					}
				}

				if match {
					r3 := make([]string, len(n2.Headers))
					copy(r3, r1)
					for i, j := range tgtMapping {
						if j > 0 {
							r3[j] = r2[i]
						}
					}
					ch <- r3
				}
			}
		}
	}()
	n2.Data = ch

	return n2, nil
}

func (n1 Node) Sort(cols []string) (n2 Node, err error) {
	isInHeader := make([]bool, len(n1.Headers))

	m := make(map[string]int)
	for _, col := range cols {
		n := slices.Index(n1.Headers, col)
		if n < 0 {
			return n2, fmt.Errorf("column missing from input data: '%s'", col)
		}
		m[col] = n
		isInHeader[n] = true
	}

	return n1.sort(func(r1, r2 []string) int {
		for _, c := range cols {
			if c := strings.Compare(r1[m[c]], r2[m[c]]); c != 0 {
				return c
			}
		}
		for i, header := range isInHeader {
			if header {
				continue
			}
			if c := strings.Compare(r1[i], r2[i]); c != 0 {
				return c
			}
		}
		return 0
	})
}

func (n1 Node) SortI(cols []string) (n2 Node, err error) {
	m := make(map[string]int)
	for _, col := range cols {
		n := slices.Index(n1.Headers, col)
		if n < 0 {
			return n2, fmt.Errorf("column missing from input data: '%s'", col)
		}
		m[col] = n
	}

	return n1.sort(func(r1, r2 []string) int {
		for _, c := range cols {
			n1, err1 := strconv.ParseInt(r1[m[c]], 0, 64)
			n2, err2 := strconv.ParseInt(r2[m[c]], 0, 64)
			if err1 != nil && err2 != nil {
				return 0
			}
			if err1 != nil {
				return 1
			}
			if err2 != nil {
				return -1
			}
			d := n1 - n2
			if d > 0 {
				return 1
			}
			if d < 0 {
				return -1
			}
		}
		return 0
	})
}
func (n1 Node) SortF(cols []string) (n2 Node, err error) {
	m := make(map[string]int)
	for _, col := range cols {
		n := slices.Index(n1.Headers, col)
		if n < 0 {
			return n2, fmt.Errorf("column missing from input data: '%s'", col)
		}
		m[col] = n
	}

	return n1.sort(func(r1, r2 []string) int {
		for _, c := range cols {
			n1, err1 := strconv.ParseFloat(r1[m[c]], 64)
			n2, err2 := strconv.ParseFloat(r2[m[c]], 64)
			if err1 != nil && err2 != nil {
				return 0
			}
			if err1 != nil {
				return 1
			}
			if err2 != nil {
				return -1
			}
			d := n1 - n2
			if d > 0 {
				return 1
			}
			if d < 0 {
				return -1
			}
		}
		return 0
	})
}

func (n1 Node) Compact(cols []string) (n2 Node, err error) {
	n2.Headers = n1.Headers
	ch := make(chan []string)
	n2.Data = ch

	if len(cols) == 0 {
		cols = n1.Headers
	}
	for _, col := range cols {
		if slices.Index(n1.Headers, col) < 0 {
			err = fmt.Errorf("column was missing from input: %v", col)
			return
		}
	}

	inclusions := make([]bool, len(n1.Headers))
	for i, h := range n1.Headers {
		inclusions[i] = slices.Index(cols, h) >= 0
	}

	go func() {
		defer close(ch)

		prev, ok := <-n1.Data
		if !ok {
			return
		}

		ch <- prev
		for row := range n1.Data {
			for i, v := range row {
				if inclusions[i] && prev[i] != v {
					ch <- row
					prev = row
					continue
				}
			}
		}
	}()

	return n2, nil
}

func (n1 Node) Rename(from []string, to []string) (n2 Node, err error) {
	n2.Headers = make([]string, len(n1.Headers))
	copy(n2.Headers, n1.Headers)

	for i, h := range from {
		j := slices.Index(n1.Headers, h)
		if j < 0 {
			return n2, fmt.Errorf("column missing from input: %v", h)
		}

		n2.Headers[j] = to[i]
	}
	n2.Data = n1.Data

	return n2, nil
}

func (n1 Node) Drop(cols []string) (n2 Node, err error) {
	for _, col := range cols {
		if slices.Index(n1.Headers, col) < 0 {
			err = fmt.Errorf("column was missing from input: %v", col)
			return
		}
	}

	j := 0
	mapping := make([]int, len(n1.Headers))
	for i, h := range n1.Headers {
		if slices.Contains(cols, h) {
			mapping[i] = -1
		} else {
			n2.Headers = append(n2.Headers, h)
			mapping[i] = j
			j++
		}
	}
	ch := make(chan []string)
	n2.Data = ch

	go func() {
		defer close(ch)

		for row := range n1.Data {
			newRow := make([]string, j)
			for i, v := range row {
				col := mapping[i]
				if col >= 0 {
					newRow[col] = v
				}
			}
			ch <- newRow
		}
	}()
	return n2, nil
}

func (n1 Node) SavePartitions(cols []string, fnameTemplate string) (n2 Node, err error) {
	for _, col := range cols {
		if slices.Index(n1.Headers, col) < 0 {
			err = fmt.Errorf("column was missing from input: %v", col)
			return
		}
	}
	n2.Headers = n1.Headers

	mapping := make([]int, len(n1.Headers))
	for i := range n1.Headers {
		mapping[i] = -1
	}
	for i, h := range cols {
		j := slices.Index(n1.Headers, h)
		mapping[j] = i
	}

	ch := make(chan []string)
	go func() {
		defer close(ch)

		files := make(map[string][][]string)
		writers := make(map[string]*csv.Writer)
		for row := range n1.Data {
			ch <- row

			key := make([]any, len(cols))
			for i := 0; i < len(n1.Headers); i++ {
				j := mapping[i]
				if j < 0 {
					continue
				}

				key[j] = row[i]
			}
			fname := fmt.Sprintf(fnameTemplate, key...)
			files[fname] = append(files[fname], row)

			if len(files[fname]) > 1000 {
				if writers[fname] == nil {
					f, err := os.Create(fname)
					if err != nil {
						panic(err)
					}
					writers[fname] = csv.NewWriter(f)
					writers[fname].Write(n1.Headers)
				}

				err = writers[fname].WriteAll(files[fname])
				if err != nil {
					panic(err)
				}
				files[fname] = nil
			}
		}
		for fname, records := range files {
			if len(records) == 0 {
				continue
			}

			if writers[fname] == nil {
				f, err := os.Create(fname)
				if err != nil {
					panic(err)
				}
				writers[fname] = csv.NewWriter(f)
				writers[fname].Write(n1.Headers)
			}

			err = writers[fname].WriteAll(records)
			if err != nil {
				panic(err)
			}
		}
		for _, wr := range writers {
			wr.Flush()
			if err = wr.Error(); err != nil {
				panic(err)
			}
		}
	}()
	n2.Data = ch

	return n2, nil
}

func main() {
	node := Node{}

	c := csv.NewReader(utfbom.SkipOnly(os.Stdin))
	headers, err := c.Read()
	if err != nil {
		log.Fatalln("unable to read headers from input file:", err)
	}
	node.Headers = headers
	ch := make(chan []string, 100)
	node.Data = ch

	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch arg {
		case "grep":
			node, err = node.Grep(os.Args[i+1], os.Args[i+2])
			i += 2
		case "grepv":
			node, err = node.Grepv(os.Args[i+1], os.Args[i+2])
			i += 2
		case "cut":
			node, err = node.Cut(strings.Split(os.Args[i+1], ","))
			i += 1
		case "shuffle":
			node, err = node.Shuffle(strings.Split(os.Args[i+1], ","))
			i += 1
		case "sort":
			node, err = node.Sort(strings.Split(os.Args[i+1], ","))
			i += 1
		case "sortf":
			node, err = node.SortF(strings.Split(os.Args[i+1], ","))
			i += 1
		case "sorti":
			node, err = node.SortI(strings.Split(os.Args[i+1], ","))
			i += 1
		case "join":
			fname := os.Args[i+1]
			cols := strings.Split(os.Args[i+2], ",")
			i += 2

			if len(cols)%2 != 0 {
				log.Fatalln(`join should have an even number of entries: source column 1,target column 1, source column 2, target column 2, etc.`)
			}
			matches := make([][2]string, len(cols)/2)
			for j := 0; j < len(cols); j += 2 {
				matches[j/2][0] = string(cols[j])
				matches[j/2][1] = string(cols[j+1])
			}
			node, err = node.Join(fname, matches)
		case "compact":
			node, err = node.Compact(strings.Split(os.Args[i+1], ","))
			i += 1
		case "drop":
			node, err = node.Drop(strings.Split(os.Args[i+1], ","))
			i += 1
		case "rename":
			node, err = node.Rename(strings.Split(os.Args[i+1], ","), strings.Split(os.Args[i+2], ","))
			i += 2
		case "save_partitions":
			node, err = node.SavePartitions(strings.Split(os.Args[i+1], ","), os.Args[i+2])
			i += 2
		default:
			log.Fatalln("unknown operation:", arg)
		}
		if err != nil {
			log.Fatalln("unable to pipeline:", err)
		}
	}

	var i int
	go func() {
		defer close(ch)
		for {
			i++
			row, err := c.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Fatalf("unable to read data from input file (row %v): %v", i, err)
			}
			ch <- row
		}
	}()

	o := csv.NewWriter(os.Stdout)
	var out [][]string
	out = append(out, node.Headers)
	for row := range node.Data {
		out = append(out, row)
		if len(out) > 1000 {
			err := o.WriteAll(out)
			if err != nil {
				log.Fatalln("error writing csv:", err)
			}
			out = nil
		}
	}
	if err := o.WriteAll(out); err != nil {
		log.Fatalln("error writing csv:", err)
	}

	o.Flush()
	if err := o.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

func main() {
	fname1 := os.Args[1]
	fname2 := os.Args[2]

	f1, err := os.Open(fname1)
	if err != nil {
		panic(err)
	}
	defer f1.Close()

	f2, err := os.Open(fname2)
	if err != nil {
		panic(err)
	}
	defer f2.Close()

	sc1 := bufio.NewScanner(f1)
	sc2 := bufio.NewScanner(f2)

	var l1, l2 string

	sc1.Scan()
	sc2.Scan()

	addCh := make(chan string)
	rmCh := make(chan string)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for added := range addCh {
			fmt.Printf("added,%v\n", added)
		}
	}()
	go func() {
		defer wg.Done()
		for removed := range rmCh {
			fmt.Printf("removed,%v\n", removed)
		}
	}()

	for {
		l1 = sc1.Text()
		l2 = sc2.Text()

		if l1 == l2 {
			ok1 := sc1.Scan()
			ok2 := sc2.Scan()
			if ok1 && ok2 {
				continue
			} else {
				break
			}
		}

		if l1 > l2 {
			addCh <- l2
			if sc2.Scan() {
				continue
			} else {
				rmCh <- l1
				break
			}
		}
		if l2 > l1 {
			rmCh <- l1
			if sc1.Scan() {
				continue
			} else {
				addCh <- l2
				break
			}
		}
	}

	for sc1.Scan() {
		rmCh <- sc1.Text()
	}
	for sc2.Scan() {
		addCh <- sc2.Text()
	}

	close(addCh)
	close(rmCh)
	wg.Wait()

	if err := sc1.Err(); err != nil {
		panic(err)
	}
	if err := sc2.Err(); err != nil {
		panic(err)
	}
}

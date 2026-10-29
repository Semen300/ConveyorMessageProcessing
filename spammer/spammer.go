package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	numberOfChans := len(cmds) + 1
	chans := make([]chan interface{}, 0, numberOfChans)
	for range numberOfChans {
		chanl := make(chan interface{})
		chans = append(chans, chanl)
	}
	var wg sync.WaitGroup
	for i, f := range cmds {
		wg.Add(1)
		go func(fun cmd, in, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			fun(in, out)
			close(out)
		}(f, chans[i], chans[i+1], &wg)
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	// 	in - string
	// 	out - User
	var wg sync.WaitGroup
	uniqueUsers := make(map[string]interface{})
	var mutex sync.Mutex
	for inp := range in {
		wg.Add(1)
		username := inp.(string)
		go func(out chan interface{}, wg *sync.WaitGroup, mutex *sync.Mutex) {
			defer wg.Done()
			user := GetUser(username)

			mutex.Lock()
			if _, notUnique := uniqueUsers[user.Email]; !notUnique {
				uniqueUsers[user.Email] = nil
				out <- user
			}
			mutex.Unlock()

		}(out, &wg, &mutex)
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	// 	in - User
	// 	out - MsgID
	var wg sync.WaitGroup
	queue := make([]User, 0)

	handle := func(batch []User, out chan interface{}, wg *sync.WaitGroup) {
		defer wg.Done()
		messages, _ := GetMessages(batch...)
		for _, message := range messages {
			out <- message
		}
	}

	for inp := range in {
		user := inp.(User)
		queue = append(queue, user)
		if len(queue) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			go handle(queue, out, &wg)
			queue = []User{}
		}
	}

	if len(queue) != 0 {
		wg.Add(1)
		go handle(queue, out, &wg)
	}

	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData

	var wg sync.WaitGroup
	limit := make(chan struct{}, HasSpamMaxAsyncRequests)

	for inp := range in {
		msgId := inp.(MsgID)
		wg.Add(1)
		limit <- struct{}{}

		go func(out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer func() {
				<-limit
			}()

			hasSpam, _ := HasSpam(MsgID(msgId))
			out <- MsgData{
				msgId,
				hasSpam,
			}

		}(out, &wg)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string
	results := make([]MsgData, 0)
	for inp := range in {
		msgData := inp.(MsgData)
		results = append(results, msgData)
	}

	sort.Slice(results, func(i, j int) bool {

		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam
		}
		return results[i].ID < results[j].ID
	})

	for _, msgData := range results {
		msgInfo := fmt.Sprintf("%t %d", msgData.HasSpam, msgData.ID)
		out <- msgInfo
	}
}

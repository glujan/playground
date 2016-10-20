package main

import (
	feed "./pb_gen"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
	// ts "github.com/golang/protobuf/ptypes/timestamp"

	"log"
)

func doFancyStuff(data []byte) {
	feed := &feed.Feed{}
	if err := proto.Unmarshal(data, feed); err != nil {
		log.Println("Can't unmarshall data!")
		return
	}
	log.Println("Doing fancy stuff with ", feed.Link)
}

func listen(conn redis.Conn) {
	for {
		reply, err := redis.Values(conn.Do("BLPOP", "feed", 0))

		if data, err := redis.Bytes(reply[1], err); err != nil {
			log.Println("Redis: Error while BLPOP: ", err)
		} else {
			go doFancyStuff(data)
		}
	}
}

func main() {
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Fatalln("Redis: Could not connect!")
	}
	defer conn.Close()

	listen(conn)
}

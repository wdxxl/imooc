package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"regexp"
	"log"
	"strconv"
	"net/url"

	"github.com/influxdata/influxdb/client/v2"
	"flag"
)

type Reader interface {
	Read(rc chan []byte)
}

type Writer interface {
	Write(wc chan *Message)
}

type ReadFromFile struct {
	path string //读取文件的路径
}

func (r *ReadFromFile) Read(rc chan []byte) {
	// 读取模块

	// 打开文件
	f, err := os.Open(r.path)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}

	// 从文件末尾开始逐行读取文件内容
	f.Seek(0, 2)
	rd := bufio.NewReader(f)

	for {
		line, err := rd.ReadBytes('\n')
		if err == io.EOF {
			time.Sleep(500 * time.Millisecond)
			continue
		} else if err != nil {
			panic(fmt.Sprintf("ReadBytes error %s", err.Error()))
		}
		rc <- line[:len(line)-1]
	}

}

type WriteToInfluxDB struct {
	influxDBDsn string // influx data source
}

func (w *WriteToInfluxDB) Write(wc chan *Message) {
	// 写入模块
	infSli :=strings.Split(w.influxDBDsn, "@")
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     infSli[0],
		Username: infSli[1],
		Password: infSli[2],
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	for value := range wc {
		// Create a new point batch
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  infSli[3],
			Precision: infSli[4],
		})
		if err != nil {
			log.Fatal(err)
		}

		// Create a point and add to batch
		// Tags: Path, Method, Scheme, Status
		tags := map[string]string{"Path": value.Method, "Method":value.Method, "Scheme":value.Scheme, "Status": value.Status}
		// Fields: UpstramTime, RequestTime, ByteSent
		fields := map[string]interface{}{
			"UpstramTime":   value.UpstreamTime,
			"RequestTime": value.RequestTime,
			"BytesSent":   value.BytesSent,
		}

		pt, err := client.NewPoint("nginx_log", tags, fields, value.TimeLocal)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)

		// Write the batch
		if err := c.Write(bp); err != nil {
			log.Fatal(err)
		}
	}

	// Close client resources
	if err := c.Close(); err != nil {
		log.Fatal(err)
	}
}

type LogProcess struct {
	rc    chan []byte
	wc    chan *Message
	read  Reader
	write Writer
}

type Message struct {
	TimeLocal                    time.Time
	BytesSent                    int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

func (l *LogProcess) Process() {
	// 解析模块
	/**
	172.0.0.12 - - [04/Mar/2018:13:49:52 +0000] http "GET /foo?query=t HTTP/1.0" 200 2133 "-" "KeepAliveClient" "-" 1.005 1.854
	*/

	r := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	loc,_ :=time.LoadLocation("Asia/Shanghai")
	for value := range l.rc {
		ret := r.FindStringSubmatch(string(value))
		if len(ret) != 14 {
			log.Println("FindStringSubmatch fail:", string(value))
			continue
		}
		message:=&Message{}
		t, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000",ret[4],loc)
		if err!=nil {
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
		}
		message.TimeLocal = t

		byteSent,_ :=strconv.Atoi(ret[8])
		message.BytesSent = byteSent

		// GET /foo?query=t HTTP/1.0
		reqSli := strings.Split(ret[6]," ")
		if len(reqSli) != 3 {
			log.Println("string.split fail:", ret[6])
		}
		message.Method = reqSli[0]

		u, err:=url.Parse(reqSli[1])
		if err!=nil{
			log.Println("url parse fail:",err)
		}
		message.Path = u.Path

		message.Scheme = ret[5]
		message.Status = ret[7]

		upstreamTime, _:= strconv.ParseFloat(ret[12], 64)
		requestTime,_:=strconv.ParseFloat(ret[13], 64)
		message.RequestTime = requestTime
		message.UpstreamTime = upstreamTime

		l.wc <- message
	}
}

// go run log_process.go -path "./access.log" -influxDsn "http://127.0.0.1:8086@imooc@imoocpass@imooc@s"
func main() {
	var path, influxDsn string
	flag.StringVar(&path, "path", "./access.log", "read file path")
	flag.StringVar(&influxDsn, "influxDsn",  "http://127.0.0.1:8086@imooc@imoocpass@imooc@s", "influxDsn path")
	flag.Parse()

	r := &ReadFromFile{path: path}
	w := &WriteToInfluxDB{influxDBDsn:influxDsn}
	lp := &LogProcess{
		make(chan []byte),
		make(chan *Message),
		r,
		w,
	}
	go lp.read.Read(lp.rc)
	go lp.Process()
	go lp.write.Write(lp.wc)

	time.Sleep(30 * time.Second)
}

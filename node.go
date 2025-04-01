package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type Value struct {
	Timestamp int64
}

type Node struct {
	mu    sync.Mutex
	store map[int]Value
	peers []string
	self  string
}

type WriteRequest struct {
	Key       int
	Timestamp int64
}

type WriteResponse struct {
	Success bool
}

func NewNode(self string, peers []string) *Node {
	return &Node{
		store: make(map[int]Value),
		peers: peers,
		self:  self,
	}
}

// Запись в локальное хранилище
func (n *Node) Write(req WriteRequest, res *WriteResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if v, exists := n.store[req.Key]; !exists || req.Timestamp > v.Timestamp {
		n.store[req.Key] = Value{Timestamp: req.Timestamp}
	}
	res.Success = true
	return nil
}

// Координатор собирает кворум
func (n *Node) WriteQuorum(req WriteRequest, res *WriteResponse) error {
	log.Printf("Получен запрос на запись с кворумом для ключа %d, временная метка: %d", req.Key, req.Timestamp)

	// Записываем данные на самом координаторе
	err := n.Write(req, res)
	if err != nil {
		log.Printf("Ошибка записи на самом координаторе: %v", err)
		return err
	}

	var wg sync.WaitGroup
	successCount := 1 // Уже записали у себя
	respCh := make(chan bool, len(n.peers))

	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				log.Printf("Ошибка подключения к %s: %v", peer, err)
				respCh <- false
				return
			}
			defer client.Close()

			var peerResp WriteResponse
			err = client.Call("Node.Write", req, &peerResp)
			if err != nil || !peerResp.Success {
				log.Printf("Ошибка при записи в %s: %v", peer, err)
				respCh <- false
				return
			}
			respCh <- true
		}(peer)
	}

	// Ожидаем ответы от реплик (таймаут 1 сек)
	timeout := time.After(1 * time.Second)
	for i := 0; i < len(n.peers); i++ {
		select {
		case success := <-respCh:
			if success {
				successCount++
			}
			if successCount >= 2 { // Успех, кворум достигнут
				res.Success = true
				log.Printf("Запись успешна, кворум собран для ключа %d", req.Key)
				return nil
			}
		case <-timeout:
			break
		}
	}

	res.Success = false
	log.Printf("Не удалось собрать кворум для ключа %d", req.Key)
	return fmt.Errorf("не удалось собрать кворум")
}

func (n *Node) Start() {
	rpc.Register(n)
	listener, err := net.Listen("tcp", n.self)
	if err != nil {
		log.Fatalf("Ошибка запуска узла: %v", err)
	}
	log.Printf("Узел запущен на %s", n.self)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Ошибка соединения: %v", err)
			continue
		}
		log.Printf("Принято соединение от %v", conn.RemoteAddr()) // Логируем успешное подключение
		go rpc.ServeConn(conn)
	}
}

// Печатает состояние хранилища
func (n *Node) printStoreState() {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("Текущее состояние хранилища на узле %s:", n.self)
	for key, value := range n.store {
		log.Printf("Ключ: %d, Значение: %d", key, value.Timestamp)
	}
}

func main() {
	var self string
	var peers string
	flag.StringVar(&self, "self", "", "Адрес этого узла (например, localhost:5000)")
	flag.StringVar(&peers, "peers", "", "Список других узлов (через запятую)")
	flag.Parse()

	if self == "" || peers == "" {
		log.Fatal("Необходимо указать --self и --peers")
	}

	peerList := strings.Split(peers, ",")
	node := NewNode(self, peerList)

	// Запускаем сервер в горутине
	go node.Start()

	// Печатаем состояние хранилища каждые 15 секунд
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		node.printStoreState()
	}
}

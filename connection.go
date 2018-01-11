package rmq

import (
	"fmt"
	"github.com/adjust/uniuri"
	"gopkg.in/redis.v3"
	_ "log"
	"strings"
	"sync/atomic"
	"time"
)

const heartbeatDuration = time.Minute

// Connection is an interface that can be used to test publishing
type Connection interface {
	OpenQueue(name string) Queue
	CollectStats(queueList []string) Stats
	GetOpenQueues() ([]string, error)
	Active() bool
}

// Connection is the entry point. Use a connection to access queues, consumers and deliveries
// Each connection has a single heartbeat shared among all consumers
type redisConnection struct {
	Name             string
	heartbeatKey     string // key to keep alive
	queuesKey        string // key to list of queues consumed by this connection
	redisClient      *redis.Client
	heartbeatStopped bool
	status           int32
}

// OpenConnectionWithRedisClient opens and returns a new connection
func OpenConnectionWithRedisClient(tag string, redisClient *redis.Client) (*redisConnection, error) {
	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	connection := &redisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  redisClient,
		status:       0,
	}

	if err := connection.updateHeartbeat(); err != nil { // checks the connection
		//atomic.StoreInt32(&connection.status, 1)
		return nil, fmt.Errorf("rmq connection failed to update heartbeat %s", connection)
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	if err := redisClient.SAdd(connectionsKey, name).Err(); err != nil {
		return nil, err
	}

	go connection.heartbeat()
	// log.Printf("rmq connection connected to %s %s:%s %d", name, network, address, db)
	return connection, nil
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag, network, address string, db int) (*redisConnection, error) {
	redisClient := redis.NewClient(&redis.Options{
		Network: network,
		Addr:    address,
		DB:      int64(db),
	})

	rc, err := OpenConnectionWithRedisClient(tag, redisClient)

	return rc, err
}

// OpenQueue opens and returns the queue with a given name
func (connection *redisConnection) OpenQueue(name string) Queue {
	connection.redisClient.SAdd(queuesKey, name)

	return newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
}

func (connection *redisConnection) Active() bool {
	a := atomic.LoadInt32(&connection.status)

	return a == 0
}

func (connection *redisConnection) CollectStats(queueList []string) Stats {
	return CollectStats(queueList, connection)
}

func (connection *redisConnection) String() string {
	return connection.Name
}

// GetConnections returns a list of all open connections
func (connection *redisConnection) GetConnections() ([]string, error) {
	result := connection.redisClient.SMembers(connectionsKey)
	if result.Err() != nil {
		return []string{}, result.Err()
	}

	return result.Val(), nil
}

// Check retuns true if the connection is currently active in terms of heartbeat
func (connection *redisConnection) Check() bool {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, connection.Name, 1)
	if err := connection.redisClient.TTL(heartbeatKey).Err(); err != nil {
		return true
	}
	return false
}

// StopHeartbeat stops the heartbeat of the connection
// it does not remove it from the list of connections so it can later be found by the cleaner
func (connection *redisConnection) StopHeartbeat() error {
	connection.heartbeatStopped = true
	return connection.redisClient.Del(connection.heartbeatKey).Err()
}

func (connection *redisConnection) Close() error {
	return connection.redisClient.SRem(connectionsKey, connection.Name).Err()
}

// GetOpenQueues returns a list of all open queues
func (connection *redisConnection) GetOpenQueues() ([]string, error) {
	result := connection.redisClient.SMembers(queuesKey)
	if result.Err() != nil {
		return []string{}, result.Err()
	}
	return result.Val(), nil
}

// CloseAllQueues closes all queues by removing them from the global list
func (connection *redisConnection) CloseAllQueues() int {
	result := connection.redisClient.Del(queuesKey)
	if result.Err() != nil {
		return 0
	}
	return int(result.Val())
}

// CloseAllQueuesInConnection closes all queues in the associated connection by removing all related keys
func (connection *redisConnection) CloseAllQueuesInConnection() error {
	return connection.redisClient.Del(connection.queuesKey).Err()
	// debug(fmt.Sprintf("connection closed all queues %s %d", connection, connection.queuesKey)) // COMMENTOUT
}

// GetConsumingQueues returns a list of all queues consumed by this connection
func (connection *redisConnection) GetConsumingQueues() ([]string, error) {
	result := connection.redisClient.SMembers(connection.queuesKey)
	if result.Err() != nil {
		return []string{}, result.Err()
	}
	return result.Val(), nil
}

// heartbeat keeps the heartbeat key alive
func (connection *redisConnection) heartbeat() {
	for {
		if connection.updateHeartbeat() != nil {
			//atomic.StoreInt32(&connection.status, 1)
			// log.Printf("rmq connection failed to update heartbeat %s", connection)
		}

		time.Sleep(time.Second)

		if connection.heartbeatStopped {
			//atomic.StoreInt32(&connection.status, 1)
			// log.Printf("rmq connection stopped heartbeat %s", connection)
			return
		}
	}
}

func (connection *redisConnection) updateHeartbeat() error {
	return connection.redisClient.Set(connection.heartbeatKey, "1", heartbeatDuration).Err()
}

// hijackConnection reopens an existing connection for inspection purposes without starting a heartbeat
func (connection *redisConnection) hijackConnection(name string) *redisConnection {
	return &redisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  connection.redisClient,
	}
}

// openQueue opens a queue without adding it to the set of queues
func (connection *redisConnection) openQueue(name string) *redisQueue {
	return newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
}

// flushDb flushes the redis database to reset everything, used in tests
func (connection *redisConnection) flushDb() {
	connection.redisClient.FlushDb()
}

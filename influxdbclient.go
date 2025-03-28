package influxdbclient

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"go.uber.org/zap"
)

const (
	BATCH_SIZE     = 3
	FLUSH_INTERVAL = 10000
	RETRY_INTERVAL = 5000
	MAX_RETRIES    = 3
)

type InfluxDBConfig struct {
	URL    string
	Token  string
	Org    string
	Bucket string
}

type InfluxDBClient struct {
	Config   InfluxDBConfig
	Logger   *zap.Logger
	client   influxdb2.Client
	queryAPI api.QueryAPI
	writeAPI api.WriteAPI
}

type Message struct {
	State                string
	CustomerID           uuid.UUID
	CustomerName         string
	SiteID               uuid.UUID
	SiteName             string
	Gateway              string
	Controller           string
	DeviceType           string
	ControllerIdentifier string
	DeviceName           string
	DeviceIdentifier     string
	Data                 map[string]any
	Timestamp            time.Time
}

// NewInfluxDBClient creates a new InfluxDB client
func NewInfluxDBClient(url, token, org, bucket string, logger *zap.Logger) *InfluxDBClient {
	config := InfluxDBConfig{
		URL:    url,
		Token:  token,
		Org:    org,
		Bucket: bucket,
	}

	return &InfluxDBClient{
		Config: config,
		Logger: logger,
	}
}

// Connected checks if the client is connected
func (c *InfluxDBClient) Connected() bool {
	return c.client != nil
}

// Connect connects to InfluxDB
func (c *InfluxDBClient) Connect() error {
	c.Logger.Info("Connecting to InfluxDB", zap.String("url", c.Config.URL), zap.String("org", c.Config.Org), zap.String("bucket", c.Config.Bucket))

	if c.client != nil {
		c.Logger.Warn("InfluxDB client already connected")
		return nil
	}

	// options := influxdb2.DefaultOptions().SetBatchSize(BATCH_SIZE)
	// options = options.SetFlushInterval(FLUSH_INTERVAL)
	// options = options.SetRetryInterval(RETRY_INTERVAL)
	// options = options.SetMaxRetries(MAX_RETRIES)

	// c.client = influxdb2.NewClientWithOptions(c.Config.URL, c.Config.Token, options)

	c.client = influxdb2.NewClient(c.Config.URL, c.Config.Token)
	c.queryAPI = c.client.QueryAPI(c.Config.Org)
	c.writeAPI = c.client.WriteAPI(c.Config.Org, c.Config.Bucket)

	// Ping the server to check connection
	ok, err := c.client.Ping(context.Background())
	if !ok || err != nil {
		c.client = nil
		return fmt.Errorf("failed to ping InfluxDB server: %w", err)
	}
	c.Logger.Debug("Successfully pinged InfluxDB server")

	// Test query to check if the connection works
	_, err = c.queryAPI.Query(context.Background(), `buckets()`)
	if err != nil {
		c.client = nil
		return fmt.Errorf("failed to query InfluxDB server: %w", err)
	}

	c.Logger.Info("Connected to InfluxDB")
	return nil

}

// Disconnect closes the connection to InfluxDB
func (c *InfluxDBClient) Disconnect() {
	if c.client == nil {
		c.Logger.Warn("No connection to InfluxDB")
		return
	}

	c.client.Close()
	c.client = nil
	c.queryAPI = nil
	c.writeAPI = nil
	c.Logger.Info("Disconnected from InfluxDB")
}

// WriteData writes data to InfluxDB
func (c *InfluxDBClient) WriteData(data Message) error {
	tags := make(map[string]string)

	tags["customerID"] = getCustomerIDTag(data.CustomerID)
	tags["customer"] = getCustomerTag(data.CustomerName)
	tags["siteID"] = getSiteIDTag(data.SiteID)
	tags["site"] = getSiteNameTag(data.SiteName)
	tags["gateway"] = getGatewayTag(data.Gateway)
	tags["controller"] = getControllerTag(data.Controller)
	tags["device_type"] = getDeviceTypeTag(data.DeviceType)
	tags["controller_identifier"] = getControllerIdentifierTag(data.ControllerIdentifier)
	tags["device_name"] = getDeviceNameTag(data.DeviceName)
	tags["device_identifier"] = getDeviceIdentifierTag(data.DeviceIdentifier)

	switch data.State {
	case "Pre":
		c.writeAPI.WritePoint(influxdb2.NewPoint("Original_MQTT", tags, data.Data, data.Timestamp))
		c.logWriterDebug(data)
	case "Post":
		c.writeAPI.WritePoint(influxdb2.NewPoint("Processed_MQTT", tags, data.Data, data.Timestamp))
		c.logWriterInfo(data)
		c.logWriterDebug(data)
	default:
		c.writeAPI.WritePoint(influxdb2.NewPoint("Original_MQTT", tags, data.Data, data.Timestamp))
		c.writeAPI.WritePoint(influxdb2.NewPoint("Processed_MQTT", tags, data.Data, data.Timestamp))
		c.logWriterInfo(data)
		c.logWriterDebug(data)
	}

	c.writeAPI.Flush()

	return nil
}

// ReadData reads data from InfluxDB using a Flux query
func (c *InfluxDBClient) ReadData(query string) ([]Message, error) {
	if c.queryAPI == nil {
		return nil, fmt.Errorf("queryAPI is not initialized")
	}

	// Execute the query
	result, err := c.queryAPI.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer result.Close()

	var messages []Message

	fmt.Println(result)

	// Process the query results
	// for result.Next() {
	// 	record := result.Record()

	// 	// Extract fields and tags from the record
	// 	data := make(map[string]interface{})
	// 	for key, value := range record.Values() {
	// 		if key != "time" && key != "result" && key != "table" {
	// 			data[key] = value
	// 		}
	// 	}

	// 	// Extract tags from the record
	// 	tags := make(map[string]string)
	// 	for key, value := range record.Values() {
	// 		if key == "customer" || key == "site" || key == "gateway" || key == "controller" || key == "device_type" || key == "controller_identifier" || key == "device_name" || key == "device_identifier" {
	// 			if strValue, ok := value.(string); ok {
	// 				tags[key] = strValue
	// 			}
	// 		}
	// 	}

	// 	// Create a Message struct from the record
	// 	message := Message{
	// 		State:                  tags["state"],
	// 		CustomerName:           tags["customer"],
	// 		SiteName:               tags["site"],
	// 		Gateway:                tags["gateway"],
	// 		Controller:             tags["controller"],
	// 		DeviceType:             tags["device_type"],
	// 		ControllerIdentifier: tags["controller_identifier"],
	// 		DeviceName:             tags["device_name"],
	// 		DeviceIdentifier:     tags["device_identifier"],
	// 		Data:                   data,
	// 		Timestamp:              record.Time(),
	// 	}

	// 	messages = append(messages, message)
	// }

	// if result.Err() != nil {
	// 	return nil, fmt.Errorf("error iterating through query results: %w", result.Err())
	// }

	return messages, nil
}

func getCustomerIDTag(customerID uuid.UUID) string {
	if customerID == uuid.Nil {
		return "Unknown customer ID"
	}

	return customerID.String()
}

func getCustomerTag(customer string) string {
	if customer == "" {
		return "Unknown customer"
	}

	return customer
}

func getSiteIDTag(siteID uuid.UUID) string {
	if siteID == uuid.Nil {
		return "Unknown site ID"
	}

	return siteID.String()
}

func getSiteNameTag(siteName string) string {
	if siteName == "" {
		return "Unknown site"
	}

	return siteName
}

func getGatewayTag(gateway string) string {
	if gateway == "" {
		return "Unknown gateway"
	}

	return gateway
}

func getControllerTag(controller string) string {
	if controller == "" {
		return "Unknown controller"
	}

	return controller
}

func getDeviceTypeTag(deviceType string) string {
	if deviceType == "" {
		return "Unknown device type"
	}

	return deviceType
}

func getControllerIdentifierTag(controllerIdentifier string) string {
	if controllerIdentifier == "" {
		return "Unknown controller serial number"
	}

	return controllerIdentifier
}

func getDeviceNameTag(deviceName string) string {
	if deviceName == "" {
		return "Unknown device name"
	}

	return deviceName
}

func getDeviceIdentifierTag(deviceIdentifier string) string {
	if deviceIdentifier == "" {
		return "Unknown device serial number"
	}

	return deviceIdentifier
}

func (c *InfluxDBClient) logWriterInfo(message Message) {
	customer := message.CustomerName
	siteName := message.SiteName
	gateway := message.Gateway
	controller := message.Controller
	deviceType := message.DeviceType
	controllerIdentifier := message.ControllerIdentifier
	deviceName := message.DeviceName
	deviceIdentifier := message.DeviceIdentifier

	c.Logger.Info(fmt.Sprintf("%s :: %s :: %s :: %s :: %s :: %s :: %s :: %s", gateway, customer, siteName, controller, controllerIdentifier, deviceType, deviceIdentifier, deviceName))
}

func (c *InfluxDBClient) logWriterDebug(message Message) {
	customer := message.CustomerName
	siteName := message.SiteName
	gateway := message.Gateway
	controller := message.Controller
	deviceType := message.DeviceType
	controllerIdentifier := message.ControllerIdentifier
	deviceName := message.DeviceName
	deviceIdentifier := message.DeviceIdentifier

	loggerMsg := fmt.Sprintf("%s :: %s :: %s :: %s :: %s :: %s :: %s :: %s", gateway, customer, siteName, controller, controllerIdentifier, deviceType, deviceIdentifier, deviceName)

	if message.State != "" {
		loggerMsg = fmt.Sprintf("%s :: %s", loggerMsg, message.State)
	}

	c.Logger.Debug(loggerMsg, zap.Any("data", message.Data))
}

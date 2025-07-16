module github.com/vmyroslav/kafka-pulse-go/adapter/confluentic

go 1.24

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.11.0
	github.com/stretchr/testify v1.10.0
	github.com/vmyroslav/kafka-pulse-go v0.0.0-20250714201313-88c0f036e6c1
)

replace (
	github.com/vmyroslav/kafka-pulse-go => ../..
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

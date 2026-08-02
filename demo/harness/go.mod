// Worker-kill demo harness.
//
// This module lives OUTSIDE the reviewed service and changes nothing in it. It
// imports the service's real Workflow, real Activity, real file-backed store
// adapter, and real command agent executor through the replace directive below.
//
// The module path is rooted under the service path on purpose: the code under
// test lives in .../temporal-maintenance/internal/temporalbeads, and Go's
// internal-package rule only admits importers whose path shares that prefix.
module github.com/sjarmak/gas-city/services/temporal-maintenance/demoharness

go 1.25.4

require (
	github.com/sjarmak/gas-city/services/temporal-maintenance v0.0.0
	go.temporal.io/api v1.63.0
	go.temporal.io/sdk v1.46.0
	golang.org/x/sys v0.45.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/go-sql-driver/mysql v1.10.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.22.0 // indirect
	github.com/nexus-rpc/nexus-proto-annotations v0.1.0 // indirect
	github.com/nexus-rpc/sdk-go v0.6.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// The reviewed service is a local, unpublished module, so it is resolved from
// disk. ../.service-checkout is a symlink that run.sh points at whatever
// checkout you set SERVICE to; that keeps one machine's directory layout out of
// this file. The checkout is READ-ONLY: the harness imports its exported seams
// and edits nothing inside it.
replace github.com/sjarmak/gas-city/services/temporal-maintenance => ../.service-checkout

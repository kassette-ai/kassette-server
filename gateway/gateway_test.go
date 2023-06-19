package gateway

import (
	. "github.com/onsi/ginkgo/v2"
)

// Create mocks to initialize gateway
type testContext struct {
	// create mocks for gateway initializers

}

func initGW() *testContext {
	// initialize gateway
	Init()
	return &testContext{}
}

var _ = Describe("Gateway Enterprise", func() {
	initGW()

	var (
		c       *testContext
		gateway *HandleT
	)

	BeforeEach(func() {
		c = &testContext{}
		//c.Setup()
		gateway = &HandleT{}
	})

	It("should not accept invalid JSON", func() {
		// Create mock request
		// Pass to gateway.getPayloadAndWriteKey(c)
		// Expect error
		gateway.getPayloadAndWriteKey(nil)
	})

})

package gateway_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "kassette.ai/kassette-server/gateway"
)

var _ = Describe("Gateway", func() {

	Init()

	var (

		//c       *testContext
		gateway *HandleT
	)

	BeforeEach(func(ctx SpecContext) {
		//c = &testContext{}
		//c.Setup()
		gateway = &HandleT{}
	})

	When("the payload is JSON", func() {

		It("should not accept invalid JSON", func() {
			// Create mock request
			// Pass to gateway.getPayloadAndWriteKey(c)
			// Expect error
			_, _, err := gateway.GetPayloadAndWriteKey(nil)
			Expect(err).To(MatchError("Invalid JSON"))
		})
	})

})

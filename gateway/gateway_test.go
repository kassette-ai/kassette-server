package gateway_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"net/http"
	"strings"

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

			request, err := http.NewRequest("POST", "/api/v1/gateway", strings.NewReader("{xxx 245 '[invalid JSON'}"))
			if err != nil {
				return
			}
			_, err = gateway.GetPayloadFromRequest(request)

			Expect(err).To(MatchError("Failed to read body from request"))

		})
	})

})

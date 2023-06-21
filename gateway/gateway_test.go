package gateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

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

			request, err := http.NewRequest("POST", "/api/v1/gateway", strings.NewReader("{xxx 245 '[invalid JSON'}"))

			_, err = gateway.GetPayloadFromRequest(request)

			Expect(err).To(MatchError("Failed to read body from request"))

		})

		It("should accept valid JSON", func() {
			// Create mock request
			// Pass to gateway.getPayloadAndWriteKey(c)
			// Expect error

			validJSON := `{"name": "John", "age": 23}`
			invalidJSON := `{"name": 'John', age: 23,}`

			fmt.Println("valid -> ", json.Valid([]byte(validJSON)))
			fmt.Println("invalid -> ", json.Valid([]byte(invalidJSON)))

			request, err := http.NewRequest("POST", "/api/v1/gateway", strings.NewReader(validJSON))

			_, err = gateway.GetPayloadFromRequest(request)

			// Expect(err).To(MatchError("Failed to read body from request"))
			Expect(err).To(BeNil())

		})
	})

})

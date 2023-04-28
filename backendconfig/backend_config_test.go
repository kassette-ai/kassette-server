package backendconfig

import (
	"fmt"
	"testing"
)

const incomingPayload = `{
  "config": {
    "a": {
      "aa": {
        "aaa": {
          "sta": "special_object_A",
          "stb": "special_object_B"
        },
        "aab": "bab"
      },
      "ab": true
    },
    "b": "special_string"
  },
  "other": "other"
}`

func TestTransform(t *testing.T) {

	fmt.Print("asdasds")

	err := json.Unmarshal([]byte(myJSON), &biggerType)
	if err != nil {
		panic(err)
	}

}

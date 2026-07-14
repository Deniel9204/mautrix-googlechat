package connector

import (
	"encoding/json"
	"testing"
)

// The JSON shapes are permanent DB contents; this pins them.
func TestUserLoginMetadataJSONShape(t *testing.T) {
	meta := UserLoginMetadata{
		Cookies:   map[string]string{"SID": "x"},
		UserAgent: "UA",
		Revision:  42,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"cookies":{"SID":"x"},"user_agent":"UA","revision":42}`
	if string(data) != want {
		t.Fatalf("got %s want %s", data, want)
	}
}

func TestMessageMetadataJSONShape(t *testing.T) {
	data, _ := json.Marshal(MessageMetadata{TimestampMicro: 1700000000000000, LastEditTime: 5})
	want := `{"ts_micro":1700000000000000,"last_edit_time":5}`
	if string(data) != want {
		t.Fatalf("got %s want %s", data, want)
	}
}

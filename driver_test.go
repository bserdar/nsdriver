package nsdriver

import (
	"testing"
)

func TestCreate(t *testing.T) {
	factory := nsDriverFactory{}

	d, err := factory.Create(map[string]interface{}{"hostname": "host",
		"keyname": "kn", "key": "k", "ssl": true})
	if err != nil {
		t.Errorf("Unexpected error:%s", err)
	}
	var driver *Driver
	var ok bool
	if driver, ok = d.(*Driver); !ok {
		t.Errorf("Wrong type")
	}
	if driver.ns.Hostname != "host" ||
		driver.ns.Keyname != "kn" ||
		driver.ns.Key != "k" ||
		driver.ns.Ssl != "s" {
		t.Errorf("Wrong values: %v", driver)
	}
}

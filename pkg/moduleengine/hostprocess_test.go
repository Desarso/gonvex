package moduleengine

import "testing"

func TestRemoteHostStatusDoesNotStartLazyManagedHost(t *testing.T) {
	host := NewRemoteHost(HostOptions{Binary: "/path/that/does/not/need/to/exist"})

	status := host.Status()
	if !status.Configured || !status.Managed {
		t.Fatalf("unexpected host configuration: %+v", status)
	}
	if status.Started || status.Running || status.Connected || status.Ready || status.Closed || status.Epoch != 0 {
		t.Fatalf("status changed lazy host state: %+v", status)
	}
}

func TestRemoteHostStatusReportsClosedHost(t *testing.T) {
	host := NewRemoteHost(HostOptions{Binary: "/path/that/does/not/need/to/exist"})
	host.closed = true

	status := host.Status()
	if !status.Closed || status.Ready || status.Error != "module host is shut down" {
		t.Fatalf("unexpected closed host status: %+v", status)
	}
}

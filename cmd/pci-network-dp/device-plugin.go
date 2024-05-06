package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"
	resource "github.com/jonkeyguan/pci-network-device-plugin/pkg/pci-network-device"
)

func main() {
	flag.Parse()
	defer glog.Flush()
	glog.Infof("Starting PCI Network Device Plugin...")
	nm := resource.NewNicsManager()
	if nm == nil {
		glog.Errorf("Unable to get instance of a NicsManager")
		return
	}
	nm.Cleanup()

	// respond to syscalls for termination
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// Discover network device(s)
	if err := nm.DiscoverNetworks(); err != nil {
		glog.Errorf("NicsManager.DiscoverNetworks() failed: %v", err)
		return
	}

	// Start server
	if err := nm.Start(); err != nil {
		glog.Errorf("NicsManager.Start() failed: %v", err)
		return
	}

	// Catch termination signals
	select {
	case sig := <-sigCh:
		glog.Infof("Received signal \"%v\", shutting down.", sig)
		nm.Stop()
		return
	}
}

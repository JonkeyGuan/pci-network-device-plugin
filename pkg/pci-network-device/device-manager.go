package resource

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

const (
	netDirectory = "/sys/class/net/"
	routePath    = "/proc/net/route"

	// Device plugin settings.
	pluginMountPath      = "/var/lib/kubelet/device-plugins"
	pluginEndpointPrefix = "nics"
	resourceName         = "pci/nics"
	name                 = "pci network device manager"
)

type NicsManager struct {
	socketFile string
	devices    map[string]pluginapi.Device // for Kubelet DP API
	grpcServer *grpc.Server
}

func NewNicsManager() *NicsManager {
	return &NicsManager{
		devices:    make(map[string]pluginapi.Device),
		socketFile: fmt.Sprintf("%s.sock", pluginEndpointPrefix),
	}
}

// GetPreferredAllocation returns a preferred set of devices to allocate
// from a list of available ones. The resulting preferred allocation is not
// guaranteed to be the allocation ultimately performed by the
// devicemanager. It is only designed to help the devicemanager make a more
// informed allocation decision when possible.
func (nm *NicsManager) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

// Implements DevicePlugin service functions
func (nm *NicsManager) ListAndWatch(empty *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	resp := new(pluginapi.ListAndWatchResponse)
	for _, dev := range nm.devices {
		resp.Devices = append(resp.Devices, &pluginapi.Device{ID: dev.ID, Health: dev.Health})
	}
	glog.Infof("ListAndWatch: send initial devices %v\n", resp)
	if err := stream.Send(resp); err != nil {
		glog.Errorf("Error. Cannot update initial device states: %v\n", err)
		nm.grpcServer.Stop()
		return err
	}

	for {
		select {
		case <-time.After(10 * time.Second):
		}

		if nm.Probe() {
			resp := new(pluginapi.ListAndWatchResponse)
			for _, dev := range nm.devices {
				resp.Devices = append(resp.Devices, &pluginapi.Device{ID: dev.ID, Health: dev.Health})
			}
			glog.Infof("ListAndWatch: send devices %v\n", resp)
			if err := stream.Send(resp); err != nil {
				glog.Errorf("Error. Cannot update device states: %v\n", err)
				nm.grpcServer.Stop()
				return err
			}
		}
	}
	// return nil
}

// Probe returns 'true' if device changes detected 'false' otherwise
func (nm *NicsManager) Probe() bool {

	changed := false
	var healthValue string

	// getNetInterfaceList is only able to get devices in default
	// namespace which means it'll not probe allocated devices, this
	// also means it'll detect newly appeared devices( newly added
	// devices or Pod released devices ).
	// TODO: find a way to detect health state of allocated devices.
	nicsNameAddress, err := getNetInterfaceList()
	if err != nil {
		glog.Errorf("Error. No pci network device found")
		return false
	}
	for name, addr := range nicsNameAddress {
		if isNetlinkStatusUp(name) {
			healthValue = pluginapi.Healthy
		} else {
			healthValue = "Unhealthy"
		}
		device := nm.devices[addr]
		if device.Health != healthValue {
			nm.devices[addr] = pluginapi.Device{ID: addr, Health: healthValue}
			changed = true
		}
	}
	return changed
}

func (nm *NicsManager) PreStartContainer(ctx context.Context, psRqt *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

// Removes existing socket if exists
func (nm *NicsManager) Cleanup() error {
	pluginEndpoint := filepath.Join(pluginMountPath, nm.socketFile)
	if err := os.Remove(pluginEndpoint); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// Reads DeviceName and gets PCI Addresses of nics
func (nm *NicsManager) DiscoverNetworks() error {

	var healthValue string
	nicsNameAddress, err := getNetInterfaceList()
	// getNetInterfaceList()
	if err != nil {
		glog.Errorf("Error. No network device found")
		return err
	}
	for name, addr := range nicsNameAddress {
		if isNetlinkStatusUp(name) {
			healthValue = pluginapi.Healthy
		} else {
			healthValue = "Unhealthy"
		}
		nm.devices[addr] = pluginapi.Device{ID: addr, Health: healthValue}
	}
	return nil
}

// Serve starts the gRPC server of the device plugin.
func (nm *NicsManager) Serve() error {
	pluginEndpoint := filepath.Join(pluginMountPath, nm.socketFile)
	os.Remove(pluginEndpoint)
	sock, err := net.Listen("unix", pluginEndpoint)
	if err != nil {
		return err
	}
	nm.grpcServer = grpc.NewServer()

	// Register network device plugin service
	registerapi.RegisterRegistrationServer(nm.grpcServer, nm)
	pluginapi.RegisterDevicePluginServer(nm.grpcServer, nm)

	go func() {
		lastCrashTime := time.Now()
		restartCount := 0

		for {
			// quite if it has been restarted too often
			// i.e. if server has crashed more than 5 times and it didn't last more than one hour each time
			if restartCount > 5 {
				// quit
				glog.Fatalf("GRPC server for '%s' has repeatedly crashed recently. Quitting", name)
			}

			glog.Infof("Starting GRPC server for '%s'", name)
			err := nm.grpcServer.Serve(sock)
			if err == nil {
				break
			}

			glog.Infof("GRPC server for '%s' crashed with error: %v", name, err)

			timeSinceLastCrash := time.Since(lastCrashTime).Seconds()
			lastCrashTime = time.Now()
			if timeSinceLastCrash > 3600 {
				// it has been one hour since the last crash.. reset the count to reflect on the frequency
				restartCount = 0
			} else {
				restartCount++
			}
		}
	}()

	// Wait for server to start by launching a blocking connection
	conn, err := nm.dial(pluginEndpoint, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()

	return nil
}

// Discovers capabable network devices
func (nm *NicsManager) Start() error {
	err := nm.Serve()
	if err != nil {
		glog.Infof("Could not start device plugin for '%s': %s", name, err)
		nm.cleanup()
		return err
	}
	glog.Infof("Starting to serve '%s' on %s", name, nm.socketFile)

	err = nm.Register()
	if err != nil {
		glog.Infof("Could not register device plugin: %s", err)
		return errors.Join(err, nm.Stop())
	}
	glog.Infof("Registered device plugin for '%s' with Kubelet", name)

	return nil
}

// dial establishes the gRPC communication with the registered device plugin.
func (nm *NicsManager) dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return net.Dial("unix", addr)
	}

	conn, err := grpc.DialContext(ctx, unixSocketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// Register registers the device plugin for the given resourceName with Kubelet.
func (nm *NicsManager) Register() error {
	conn, err := nm.dial(pluginapi.KubeletSocket, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	reqt := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(nm.socketFile),
		ResourceName: string(resourceName),
		Options: &pluginapi.DevicePluginOptions{
			GetPreferredAllocationAvailable: true,
		},
	}

	_, err = client.Register(context.Background(), reqt)
	if err != nil {
		return err
	}
	return nil
}

func (nm *NicsManager) GetInfo(ctx context.Context, rqt *registerapi.InfoRequest) (*registerapi.PluginInfo, error) {
	return &registerapi.PluginInfo{Type: registerapi.DevicePlugin, Name: resourceName, Endpoint: filepath.Join(pluginMountPath, nm.socketFile), SupportedVersions: []string{"v1beta1"}}, nil
}

func (nm *NicsManager) NotifyRegistrationStatus(ctx context.Context, regstat *registerapi.RegistrationStatus) (*registerapi.RegistrationStatusResponse, error) {
	out := new(registerapi.RegistrationStatusResponse)
	if regstat.PluginRegistered {
		glog.Infof("Plugin: %s gets registered successfully at Kubelet\n", nm.socketFile)
	} else {
		glog.Infof("Plugin:%s failed to registered at Kubelet: %v; shutting down.\n", nm.socketFile, regstat.Error)
		nm.Stop()
	}
	return out, nil
}

func (nm *NicsManager) Stop() error {
	glog.Infof("Stopping PCI Network Device Plugin gRPC server..")
	if nm.grpcServer == nil {
		return nil
	}

	nm.grpcServer.Stop()
	nm.grpcServer = nil

	return nm.cleanup()
}

// Removes existing socket if exists
func (nm *NicsManager) cleanup() error {
	pluginEndpoint := filepath.Join(pluginMountPath, nm.socketFile)
	if err := os.Remove(pluginEndpoint); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// Allocate passes the PCI Addr(s) as an env variable to the requesting container
func (nm *NicsManager) Allocate(ctx context.Context, rqt *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	resp := new(pluginapi.AllocateResponse)
	pciAddrs := ""
	for _, container := range rqt.ContainerRequests {
		containerResp := new(pluginapi.ContainerAllocateResponse)
		for _, id := range container.DevicesIDs {
			glog.Infof("DeviceID in Allocate: %v", id)
			dev, ok := nm.devices[id]
			if !ok {
				glog.Errorf("Error. Invalid allocation request with non-existing device %s", id)
				return nil, fmt.Errorf("Error. Invalid allocation request with non-existing device %s", id)
			}
			if dev.Health != pluginapi.Healthy {
				glog.Errorf("Error. Invalid allocation request with unhealthy device %s", id)
				return nil, fmt.Errorf("Error. Invalid allocation request with unhealthy device %s", id)
			}

			pciAddrs = pciAddrs + id + ","
		}

		glog.Infof("PCI Addrs allocated: %s", pciAddrs)
		envmap := make(map[string]string)
		envmap["PCI_RESOURCE_NICS"] = pciAddrs

		containerResp.Envs = envmap
		resp.ContainerResponses = append(resp.ContainerResponses, containerResp)
	}
	return resp, nil
}

func (nm *NicsManager) GetDevicePluginOptions(ctx context.Context, empty *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{
		PreStartRequired: false,
	}, nil
}

// Returns a list of network interface names as string
func getNetInterfaceList() (map[string]string, error) {
	nicsNameAddress := make(map[string]string)
	var defaultInterface string

	netDevices, err := os.ReadDir(netDirectory)
	if err != nil {
		glog.Errorf("Error. Cannot read %s for network device names. Err: %v", netDirectory, err)
		return nicsNameAddress, err
	}

	if len(netDevices) < 1 {
		glog.Errorf("Error. No network device found in %s directory", netDirectory)
		return nicsNameAddress, err
	}

	routeFile, err := os.Open(routePath)
	if err != nil {
		glog.Errorf("Error. Cannot read %s for default route interface. Err: %v", routePath, err)
		return nicsNameAddress, err
	}
	defer routeFile.Close()

	scanner := bufio.NewScanner(routeFile)
	for scanner.Scan() {
		scanner.Scan()
		defaultInterface = strings.Split(scanner.Text(), "\t")[0]
		break
	}

	for _, dev := range netDevices {

		if dev.Name() == defaultInterface {
			glog.Infof("Skipping default interface %s ", defaultInterface)
			continue
		}

		subsystemPath := filepath.Join(netDirectory, dev.Name(), "device", "subsystem")
		glog.Infof("Checking for file %s ", subsystemPath)

		subsystemInfo, err := os.Readlink(subsystemPath)
		if err != nil {
			glog.Infof("Cannot read subsystem symbolic link - Skipping: %v", err)
			continue
		}

		if strings.Contains(subsystemInfo, "pci") {
			devicePath := filepath.Join(netDirectory, dev.Name())
			deviceInfo, err := os.Readlink(devicePath)
			if err != nil {
				glog.Infof("Cannot read device symbolic link - Skipping: %v", err)
				continue
			}
			glog.Infof("deviceInfo: %s", deviceInfo)
			pciStr := deviceInfo[len("../../devices/pci0000:00/"):]
			pciAddr := strings.Split(pciStr, "/")
			nicsNameAddress[dev.Name()] = pciAddr[len(pciAddr)-3]
			glog.Infof("name: %s, pci address: %s", dev.Name(), nicsNameAddress[dev.Name()])
		}
	}

	return nicsNameAddress, nil
}

// isNetlinkStatusUp returns 'false' if 'operstate' is not "up" for a Linux netowrk device
func isNetlinkStatusUp(dev string) bool {
	opsFile := filepath.Join(netDirectory, dev, "operstate")
	bytes, err := os.ReadFile(opsFile)
	if err != nil || strings.TrimSpace(string(bytes)) != "up" {
		return false
	}
	return true
}

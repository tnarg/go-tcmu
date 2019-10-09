// tcmu is a package that connects to the TCM in Userspace kernel module, a part of the LIO stack. It provides the
// ability to emulate a SCSI storage device in pure Go.
package tcmu

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

const (
	configDirFmt = "/sys/kernel/config/target/core/user_%d"
	scsiDir      = "/sys/kernel/config/target/loopback"
)

type Device struct {
	scsi    *SCSIHandler
	devPath string

	hbaDir     string
	deviceName string

	uiof     *os.File
	pollDone chan struct{}
	pollers  *sync.WaitGroup
	mapsize  uint64
	mmap     []byte
	cmdChan  chan *SCSICmd
	respChan chan SCSIResponse
	cmdTail  uint32
}

// WWN provides two WWNs, one for the device itself and one for the loopback
// device created for the kernel.
type WWN interface {
	DeviceID() string
	NexusID() string
}

func (d *Device) GetDevConfig() string {
	return fmt.Sprintf("go-tcmu//%s", d.scsi.VolumeName)
}

func (d *Device) Sizes() DataSizes {
	return d.scsi.DataSizes
}

// OpenTCMUDevice creates the virtual device based on the details in the SCSIHandler, eventually creating a device under devPath (eg, "/dev") with the file name scsi.VolumeName.
// The returned Device represents the open device connection to the kernel, and must be closed.
func OpenTCMUDevice(devPath string, scsi *SCSIHandler) (*Device, error) {
	d := &Device{
		scsi:     scsi,
		devPath:  devPath,
		pollDone: make(chan struct{}),
		pollers:  &sync.WaitGroup{},
		hbaDir:   fmt.Sprintf(configDirFmt, scsi.HBA),
	}
	err := d.Close()
	if err != nil {
		return nil, err
	}
	if err := d.preEnableTcmu(); err != nil {
		return nil, err
	}
	if err := d.start(); err != nil {
		return nil, err
	}

	return d, d.postEnableTcmu()
}

func (d *Device) Close() error {
	err := d.teardown()
	if err != nil {
		return err
	}
	if d.uiof != nil {
		close(d.pollDone)
		d.pollers.Wait()
		d.uiof.Close()
	}
	return nil
}

func (d *Device) preEnableTcmu() error {
	err := writeLines(path.Join(d.hbaDir, d.scsi.VolumeName, "control"), []string{
		fmt.Sprintf("dev_size=%d", d.scsi.DataSizes.VolumeSize),
		fmt.Sprintf("dev_config=%s", d.GetDevConfig()),
		fmt.Sprintf("hw_block_size=%d", d.scsi.DataSizes.BlockSize),
		"async=1",
	})
	if err != nil {
		return err
	}

	return writeLines(path.Join(d.hbaDir, d.scsi.VolumeName, "enable"), []string{
		"1",
	})
}

func (d *Device) getSCSIPrefixAndWnn() (string, string) {
	return path.Join(scsiDir, d.scsi.WWN.DeviceID(), "tpgt_1"), d.scsi.WWN.NexusID()
}

func (d *Device) getLunPath(prefix string) string {
	return path.Join(prefix, "lun", fmt.Sprintf("lun_%d", d.scsi.LUN))
}

func (d *Device) postEnableTcmu() error {
	prefix, nexusWnn := d.getSCSIPrefixAndWnn()

	err := writeLines(path.Join(prefix, "nexus"), []string{
		nexusWnn,
	})
	if err != nil {
		return err
	}

	lunPath := d.getLunPath(prefix)
	zap.L().Sugar().Debugf("Creating directory: %s", lunPath)
	if err := os.MkdirAll(lunPath, 0755); err != nil && !os.IsExist(err) {
		return err
	}

	zap.L().Sugar().Debugf("Linking: %s => %s", path.Join(lunPath, d.scsi.VolumeName), path.Join(d.hbaDir, d.scsi.VolumeName))
	if err := os.Symlink(path.Join(d.hbaDir, d.scsi.VolumeName), path.Join(lunPath, d.scsi.VolumeName)); err != nil {
		return err
	}

	return d.createDevEntry()
}

func (d *Device) createDevEntry() error {
	os.MkdirAll(d.devPath, 0755)

	dev := filepath.Join(d.devPath, d.scsi.VolumeName)

	if _, err := os.Stat(dev); err == nil {
		return fmt.Errorf("Device %s already exists, can not create", dev)
	}

	tgt, _ := d.getSCSIPrefixAndWnn()

	address, err := ioutil.ReadFile(path.Join(tgt, "address"))
	if err != nil {
		return err
	}

	found := false
	matches := []string{}
	path := fmt.Sprintf("/sys/bus/scsi/devices/%s*/block/*/dev", strings.TrimSpace(string(address)))
	for i := 0; i < 30; i++ {
		var err error
		matches, err = filepath.Glob(path)
		if len(matches) > 0 && err == nil {
			found = true
			break
		}

		zap.L().Sugar().Debugf("Waiting for %s", path)
		time.Sleep(1 * time.Second)
	}

	if !found {
		return fmt.Errorf("Failed to find %s", path)
	}

	if len(matches) == 0 {
		return fmt.Errorf("Failed to find %s", path)
	}

	if len(matches) > 1 {
		return fmt.Errorf("Too many matches for %s, found %d", path, len(matches))
	}

	majorMinor, err := ioutil.ReadFile(matches[0])
	if err != nil {
		return err
	}

	parts := strings.Split(strings.TrimSpace(string(majorMinor)), ":")
	if len(parts) != 2 {
		return fmt.Errorf("Invalid major:minor string %s", string(majorMinor))
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return err
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return err
	}

	zap.L().Sugar().Debugf("Creating device %s %d:%d", dev, major, minor)
	return mknod(dev, major, minor)
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0600
	fileMode |= syscall.S_IFBLK
	dev := int((major << 8) | (minor & 0xff) | ((minor & 0xfff00) << 12))

	return syscall.Mknod(device, uint32(fileMode), dev)
}

func writeLines(target string, lines []string) error {
	dir := path.Dir(target)
	if stat, err := os.Stat(dir); os.IsNotExist(err) {
		zap.L().Sugar().Debugf("Creating directory: %s", dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	} else if !stat.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}

	for _, line := range lines {
		content := []byte(line + "\n")
		zap.L().Sugar().Debugf("Setting %s: %s", target, line)
		if err := ioutil.WriteFile(target, content, 0755); err != nil {
			zap.L().Sugar().Errorf("Failed to write %s to %s: %v", line, target, err)
			return err
		}
	}

	return nil
}

func (d *Device) start() (err error) {
	err = d.findDevice()
	if err != nil {
		return
	}
	d.cmdChan = make(chan *SCSICmd, 5)
	d.respChan = make(chan SCSIResponse, 5)
	d.pollers.Add(1)
	go d.beginPoll()
	d.scsi.DevReady(d.cmdChan, d.respChan)
	return
}

func (d *Device) findDevice() error {
	err := filepath.Walk("/dev", func(path string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() && path != "/dev" {
			return filepath.SkipDir
		}
		if !strings.HasPrefix(i.Name(), "uio") {
			return nil
		}
		sysfile := fmt.Sprintf("/sys/class/uio/%s/name", i.Name())
		bytes, err := ioutil.ReadFile(sysfile)
		if err != nil {
			return err
		}
		split := strings.SplitN(strings.TrimRight(string(bytes), "\n"), "/", 4)
		if split[0] != "tcm-user" {
			// Not a TCM device
			zap.L().Sugar().Debugf("%s is not a tcm-user device", i.Name())
			return nil
		}
		if split[3] != d.GetDevConfig() {
			// Not a TCM device
			zap.L().Sugar().Debugf("%s is not our tcm-user device", i.Name())
			return nil
		}
		err = d.openDevice(split[1], split[2], i.Name())
		if err != nil {
			return err
		}
		return filepath.SkipDir
	})
	if err == filepath.SkipDir {
		return nil
	}
	return err
}

func (d *Device) openDevice(user string, vol string, uio string) error {
	var uioFd int
	var err error
	d.deviceName = vol

	fname := fmt.Sprintf("/dev/%s", uio)
	uioFd, err = syscall.Open(fname, syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	bytes, err := ioutil.ReadFile(fmt.Sprintf("/sys/class/uio/%s/maps/map0/size", uio))
	if err != nil {
		return err
	}
	d.mapsize, err = strconv.ParseUint(strings.TrimRight(string(bytes), "\n"), 0, 64)
	if err != nil {
		return err
	}
	d.uiof = os.NewFile(uintptr(uioFd), fname)
	d.mmap, err = syscall.Mmap(uioFd, 0, int(d.mapsize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	d.cmdTail = d.mbCmdTail()
	d.debugPrintMb()
	return err
}

func (d *Device) debugPrintMb() {
	zap.L().Sugar().Debugf("Got a TCMU mailbox, version: %d\n", d.mbVersion())
	zap.L().Sugar().Debugf("mapsize: %d\n", d.mapsize)
	zap.L().Sugar().Debugf("mbFlags: %d\n", d.mbFlags())
	zap.L().Sugar().Debugf("mbCmdrOffset: %d\n", d.mbCmdrOffset())
	zap.L().Sugar().Debugf("mbCmdrSize: %d\n", d.mbCmdrSize())
	zap.L().Sugar().Debugf("mbCmdHead: %d\n", d.mbCmdHead())
	zap.L().Sugar().Debugf("mbCmdTail: %d\n", d.mbCmdTail())
}

func (d *Device) teardown() error {
	dev := filepath.Join(d.devPath, d.scsi.VolumeName)
	tpgtPath, _ := d.getSCSIPrefixAndWnn()
	lunPath := d.getLunPath(tpgtPath)

	/*
		We're removing:
		/sys/kernel/config/target/loopback/naa.<id>/tpgt_1/lun/lun_0/<volume name>
		/sys/kernel/config/target/loopback/naa.<id>/tpgt_1/lun/lun_0
		/sys/kernel/config/target/loopback/naa.<id>/tpgt_1
		/sys/kernel/config/target/loopback/naa.<id>
		/sys/kernel/config/target/core/user_42/<volume name>
	*/
	pathsToRemove := []string{
		path.Join(lunPath, d.scsi.VolumeName),
		lunPath,
		tpgtPath,
		path.Dir(tpgtPath),
		path.Join(d.hbaDir, d.scsi.VolumeName),
	}

	for _, p := range pathsToRemove {
		err := remove(p)
		if err != nil {
			return err
		}
	}

	// Should be cleaned up automatically, but if it isn't remove it
	if _, err := os.Stat(dev); err == nil {
		err := remove(dev)
		if err != nil {
			return err
		}
	}

	return nil
}

func removeAsync(path string, done chan<- error) {
	zap.L().Sugar().Debugf("Removing: %s", path)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		zap.L().Sugar().Errorf("Unable to remove: %v", path)
		done <- err
	}
	zap.L().Sugar().Debugf("Removed: %s", path)
	done <- nil
}

func remove(path string) error {
	done := make(chan error)
	go removeAsync(path, done)
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("Timeout trying to delete %s.", path)
	}
}

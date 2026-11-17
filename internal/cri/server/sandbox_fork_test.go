// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package server

import (
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
	"testing"
)

func TestInheritedConfiguration(t *testing.T) {
	source := &runtime.ContainerConfig{Image: &runtime.ImageSpec{Image: "image"}, Command: []string{"work"}}
	same := &runtime.ContainerConfig{Image: &runtime.ImageSpec{Image: "image"}, Command: []string{"work"}}
	if err := validateInheritedConfig(same, source); err != nil {
		t.Fatal(err)
	}
	for _, config := range []*runtime.ContainerConfig{
		{Image: &runtime.ImageSpec{Image: "other"}, Command: []string{"work"}},
		{Image: &runtime.ImageSpec{Image: "image"}, Command: []string{"other"}},
		{Tty: true}, {Stdin: true}, {Mounts: []*runtime.Mount{{ContainerPath: "/volume"}}},
		{Linux: &runtime.LinuxContainerConfig{Resources: &runtime.LinuxContainerResources{MemoryLimitInBytes: 1024}}},
	} {
		if err := validateInheritedConfig(config, source); err == nil {
			t.Fatalf("accepted incompatible configuration: %+v", config)
		}
	}
}

func TestInheritedEnvironmentOrder(t *testing.T) {
	a := &runtime.ContainerConfig{Envs: []*runtime.KeyValue{{Key: "A", Value: "1"}, {Key: "B", Value: "2"}}}
	b := &runtime.ContainerConfig{Envs: []*runtime.KeyValue{{Key: "B", Value: "2"}, {Key: "A", Value: "1"}}}
	if err := validateInheritedConfig(a, b); err != nil {
		t.Fatal(err)
	}
	b.Envs[0].Value = "changed"
	if err := validateInheritedConfig(a, b); err == nil {
		t.Fatal("changed environment accepted")
	}
}

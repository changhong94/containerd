// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package sandbox

import (
	"context"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"testing"
)

type namespaceStore struct{ sandbox.Store }

func (*namespaceStore) Get(ctx context.Context, id string) (sandbox.Sandbox, error) {
	ns, _ := namespaces.Namespace(ctx)
	if ns != "owner" || id != "sandbox" {
		return sandbox.Sandbox{}, errdefs.ErrNotFound
	}
	return sandbox.Sandbox{ID: id, Sandboxer: "vmm"}, nil
}
func TestForkControllerNamespace(t *testing.T) {
	service := &controllerService{store: &namespaceStore{}, sc: map[string]sandbox.Controller{"vmm": nil}}
	if _, err := service.controllerForSandbox(namespaces.WithNamespace(context.Background(), "owner"), "vmm", "sandbox"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ ns, controller string }{{"other", "vmm"}, {"owner", "wrong"}} {
		if _, err := service.controllerForSandbox(namespaces.WithNamespace(context.Background(), test.ns), test.controller, "sandbox"); !errdefs.IsNotFound(err) {
			t.Fatalf("%+v: %v", test, err)
		}
	}
}

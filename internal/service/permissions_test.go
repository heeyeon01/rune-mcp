package service

import (
	"context"
	"errors"
	"testing"

	"github.com/CryptoLabInc/rune-mcp/internal/domain"
)

// Permissions requires a wired Vault (secure mode). With none it must fail
// cleanly with a vault-connection error rather than nil-dereferencing s.Vault.
func TestPermissions_RequiresVault(t *testing.T) {
	svc := NewLifecycleService() // no Vault wired
	_, err := svc.Permissions(context.Background(), PermissionsArgs{})
	if err == nil {
		t.Fatal("want an error when the Vault is not wired")
	}
	var re *domain.RuneError
	if !errors.As(err, &re) || re.Code != domain.CodeVaultConnection {
		t.Fatalf("err = %v, want *domain.RuneError{Code: CodeVaultConnection}", err)
	}
}

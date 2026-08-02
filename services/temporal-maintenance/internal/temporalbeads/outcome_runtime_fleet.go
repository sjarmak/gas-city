package temporalbeads

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type OutcomeStoreFailure struct {
	StoreRef string `json:"store_ref"`
	Stage    string `json:"stage"`
	Err      error  `json:"-"`
}

func (f OutcomeStoreFailure) Error() string {
	return fmt.Sprintf("%s %s: %v", f.StoreRef, f.Stage, f.Err)
}

func CoordinatorOutcomeTaskQueueForStore(storeRef string) (string, error) {
	if err := validateStoreRef(storeRef); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(storeRef))
	return CoordinatorOutcomeTaskQueue + "-" + hex.EncodeToString(sum[:6]), nil
}

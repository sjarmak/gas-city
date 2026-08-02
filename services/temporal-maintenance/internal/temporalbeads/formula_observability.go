package temporalbeads

import (
	"fmt"

	"go.temporal.io/sdk/temporal"
)

var (
	formulaNameSearchKey       = temporal.NewSearchAttributeKeyKeyword("GasCityFormulaName")
	formulaHashSearchKey       = temporal.NewSearchAttributeKeyKeyword("GasCityFormulaHash")
	formulaVersionSearchKey    = temporal.NewSearchAttributeKeyKeyword("GasCityFormulaVersion")
	formulaRootSearchKey       = temporal.NewSearchAttributeKeyKeyword("GasCityFormulaRoot")
	formulaStepSearchKey       = temporal.NewSearchAttributeKeyKeyword("GasCityFormulaStep")
	formulaRigSearchKey        = temporal.NewSearchAttributeKeyKeyword("GasCityRig")
	formulaBeadSearchKey       = temporal.NewSearchAttributeKeyKeyword("GasCityBead")
	formulaGenerationSearchKey = temporal.NewSearchAttributeKeyInt64("GasCityGeneration")
)

// FormulaActivityID is stable across Activity retries and Worker restarts.
func FormulaActivityID(formula FormulaRef, generation int64) (string, error) {
	if err := formula.Validate(); err != nil {
		return "", err
	}
	if generation <= 0 {
		return "", fmt.Errorf("generation must be positive")
	}
	return fmt.Sprintf(
		"formula/%s/%s/g%d",
		formula.RootID,
		formula.StepKey,
		generation,
	), nil
}

// FormulaSearchAttributeUpdates returns the registered, filterable topology
// projection used by Temporal Visibility and Web.
func FormulaSearchAttributeUpdates(
	event ReadyEvent,
) []temporal.SearchAttributeUpdate {
	return []temporal.SearchAttributeUpdate{
		formulaNameSearchKey.ValueSet(event.Formula.Name),
		formulaHashSearchKey.ValueSet(event.Formula.Hash),
		formulaVersionSearchKey.ValueSet(event.Formula.Version),
		formulaRootSearchKey.ValueSet(event.Formula.RootID),
		formulaStepSearchKey.ValueSet(event.Formula.StepKey),
		formulaRigSearchKey.ValueSet(event.Formula.Rig),
		formulaBeadSearchKey.ValueSet(event.BeadID),
		formulaGenerationSearchKey.ValueSet(event.Generation),
	}
}

// FormulaSearchAttributes builds the start-time Visibility attributes.
func FormulaSearchAttributes(event ReadyEvent) temporal.SearchAttributes {
	return temporal.NewSearchAttributes(FormulaSearchAttributeUpdates(event)...)
}

// FormulaMemo exposes the exact snapshotted formula step in Temporal Web.
//
// Memo is used instead of custom Search Attributes so shadow deployment does
// not require namespace-level attribute registration.
func FormulaMemo(event ReadyEvent) map[string]interface{} {
	memo := map[string]interface{}{
		"GasCityFormulaName":    event.Formula.Name,
		"GasCityFormulaHash":    event.Formula.Hash,
		"GasCityFormulaVersion": event.Formula.Version,
		"GasCityFormulaRoot":    event.Formula.RootID,
		"GasCityFormulaStep":    event.Formula.StepKey,
		"GasCityRig":            event.Formula.Rig,
		"GasCityBead":           event.BeadID,
		"GasCityGeneration":     event.Generation,
	}
	if event.Formula.ParentWorkflowID != "" {
		memo["GasCityParentWorkflowID"] = event.Formula.ParentWorkflowID
	}
	if event.Formula.ParentRunID != "" {
		memo["GasCityParentRunID"] = event.Formula.ParentRunID
	}
	return memo
}

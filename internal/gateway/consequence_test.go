package gateway

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderConsequenceSummary(t *testing.T) {
	tv := ToolVersion{
		Name:        "graph.email.send",
		EffectClass: EffectNonIdempotentWrite,
		ConsequenceTemplate: json.RawMessage(`{
			"localized_templates":{
				"en":"Send quotation {{quote}} to {{recipient}}",
				"pt":"Enviar cotação {{quote}} para {{recipient}}"
			},
			"argument_paths":{"quote":"/quotation/id","recipient":"/recipients/0"}
		}`),
	}
	rendered, err := renderConsequenceSummary(tv, []byte(`{"quotation":{"id":184},"recipients":["buyer@example.test"],"body":"operator-only body"}`))
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"en":"Send quotation 184 to buyer@example.test",
		"pt":"Enviar cotação 184 para buyer@example.test"
	}`, string(rendered))
	assert.NotContains(t, string(rendered), "operator-only body")
}

func TestValidateConsequenceTemplateFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		template string
		want     string
	}{
		{name: "missing Portuguese", template: `{"localized_templates":{"en":"Send {{recipient}}"},"argument_paths":{"recipient":"/to"}}`, want: "en and pt"},
		{name: "unbound placeholder", template: `{"localized_templates":{"en":"Send {{recipient}}","pt":"Enviar {{recipient}}"},"argument_paths":{}}`, want: "requires an RFC 6901"},
		{name: "unused field", template: `{"localized_templates":{"en":"Send","pt":"Enviar"},"argument_paths":{"recipient":"/to"}}`, want: "not used"},
		{name: "invalid pointer", template: `{"localized_templates":{"en":"Send {{recipient}}","pt":"Enviar {{recipient}}"},"argument_paths":{"recipient":"/bad~2path"}}`, want: "invalid ~ escape"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConsequenceTemplate(ToolVersion{Name: "write", EffectClass: EffectNonIdempotentWrite, ConsequenceTemplate: []byte(tt.template)})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestRenderConsequenceSummaryRejectsMissingOrStructuredSafeField(t *testing.T) {
	tv := ToolVersion{
		Name:        "write",
		EffectClass: EffectNonIdempotentWrite,
		ConsequenceTemplate: json.RawMessage(`{
			"localized_templates":{"en":"Update {{target}}","pt":"Atualizar {{target}}"},
			"argument_paths":{"target":"/target"}
		}`),
	}
	_, err := renderConsequenceSummary(tv, []byte(`{}`))
	assert.ErrorContains(t, err, "is absent")
	_, err = renderConsequenceSummary(tv, []byte(`{"target":{"secret":"not customer safe"}}`))
	assert.ErrorContains(t, err, "only string, number, or boolean")
}

package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	maxConsequenceTemplateBytes = 1000
	maxConsequenceSummaryBytes  = 2000
)

var consequencePlaceholderPattern = regexp.MustCompile(`\{\{([A-Za-z][A-Za-z0-9_]*)\}\}`)

type consequenceTemplate struct {
	LocalizedTemplates map[string]string `json:"localized_templates"`
	ArgumentPaths      map[string]string `json:"argument_paths"`
}

// validateConsequenceTemplate enforces the founder-approved ARCH-022 contract:
// every write descriptor has immutable English and Portuguese customer-safe
// wording, and every placeholder is explicitly bound to one argument pointer.
//
//nolint:gocyclo // Fail-closed validation enumerates every template/placeholder safety invariant explicitly.
func validateConsequenceTemplate(tv ToolVersion) error {
	if tv.EffectClass == EffectReadOnly && (len(tv.ConsequenceTemplate) == 0 || bytes.Equal(tv.ConsequenceTemplate, []byte("{}"))) {
		return nil
	}
	var template consequenceTemplate
	if err := json.Unmarshal(tv.ConsequenceTemplate, &template); err != nil {
		return fmt.Errorf("tool %s: consequence_summary_template is not valid JSON: %w", tv.Name, err)
	}
	if tv.EffectClass != EffectReadOnly {
		if strings.TrimSpace(template.LocalizedTemplates["en"]) == "" || strings.TrimSpace(template.LocalizedTemplates["pt"]) == "" {
			return fmt.Errorf("tool %s: write tools require non-empty en and pt consequence summary templates", tv.Name)
		}
	}
	locales := make([]string, 0, len(template.LocalizedTemplates))
	for locale := range template.LocalizedTemplates {
		locales = append(locales, locale)
	}
	sort.Strings(locales)
	for _, locale := range locales {
		text := template.LocalizedTemplates[locale]
		if len(text) > maxConsequenceTemplateBytes {
			return fmt.Errorf("tool %s: consequence template %q exceeds %d bytes", tv.Name, locale, maxConsequenceTemplateBytes)
		}
		matches := consequencePlaceholderPattern.FindAllStringSubmatch(text, -1)
		localeFields := make(map[string]struct{}, len(matches))
		stripped := consequencePlaceholderPattern.ReplaceAllString(text, "")
		if strings.Contains(stripped, "{{") || strings.Contains(stripped, "}}") {
			return fmt.Errorf("tool %s: consequence template %q contains an invalid placeholder", tv.Name, locale)
		}
		for _, match := range matches {
			field := match[1]
			path, ok := template.ArgumentPaths[field]
			if !ok || !strings.HasPrefix(path, "/") {
				return fmt.Errorf("tool %s: consequence placeholder %q requires an RFC 6901 argument path", tv.Name, field)
			}
			if _, err := parseJSONPointer(path); err != nil {
				return fmt.Errorf("tool %s: consequence argument path for %q: %w", tv.Name, field, err)
			}
			localeFields[field] = struct{}{}
		}
		for field := range template.ArgumentPaths {
			if _, ok := localeFields[field]; !ok {
				return fmt.Errorf("tool %s: consequence argument path %q is not used by locale %q", tv.Name, field, locale)
			}
		}
	}
	return nil
}

// renderConsequenceSummary renders localized customer-safe text from the
// already schema-validated business arguments. It runs before BeginInvocation,
// so a write cannot cross the effect boundary without its immutable summary.
//
//nolint:gocyclo // Rendering keeps pointer resolution, scalar gating, localization, and bounds in one pre-effect gate.
func renderConsequenceSummary(tv ToolVersion, arguments []byte) ([]byte, error) {
	if tv.EffectClass == EffectReadOnly {
		return nil, nil
	}
	if err := validateConsequenceTemplate(tv); err != nil {
		return nil, err
	}
	var template consequenceTemplate
	if err := json.Unmarshal(tv.ConsequenceTemplate, &template); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(arguments))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode consequence arguments: %w", err)
	}
	fields := make(map[string]string, len(template.ArgumentPaths))
	for field, pointer := range template.ArgumentPaths {
		resolved, err := resolveJSONPointer(value, pointer)
		if err != nil {
			return nil, fmt.Errorf("render consequence field %q: %w", field, err)
		}
		switch v := resolved.(type) {
		case string:
			fields[field] = v
		case json.Number:
			fields[field] = v.String()
		case bool:
			fields[field] = strconv.FormatBool(v)
		default:
			return nil, fmt.Errorf("render consequence field %q: only string, number, or boolean values are customer-safe", field)
		}
	}
	out := make(map[string]string, len(template.LocalizedTemplates))
	for locale, text := range template.LocalizedTemplates {
		for field, value := range fields {
			text = strings.ReplaceAll(text, "{{"+field+"}}", value)
		}
		if strings.Contains(text, "{{") || strings.Contains(text, "}}") {
			return nil, fmt.Errorf("render consequence locale %q left an unresolved placeholder", locale)
		}
		out[locale] = text
	}
	rendered, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if len(rendered) > maxConsequenceSummaryBytes {
		return nil, fmt.Errorf("rendered consequence summary exceeds %d bytes", maxConsequenceSummaryBytes)
	}
	return rendered, nil
}

func parseJSONPointer(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, fmt.Errorf("root pointer is not allowed")
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, fmt.Errorf("must begin with /")
	}
	parts := strings.Split(pointer[1:], "/")
	for i, part := range parts {
		var decoded strings.Builder
		for j := 0; j < len(part); j++ {
			if part[j] != '~' {
				decoded.WriteByte(part[j])
				continue
			}
			if j+1 >= len(part) || (part[j+1] != '0' && part[j+1] != '1') {
				return nil, fmt.Errorf("contains an invalid ~ escape")
			}
			j++
			if part[j] == '0' {
				decoded.WriteByte('~')
			} else {
				decoded.WriteByte('/')
			}
		}
		parts[i] = decoded.String()
	}
	return parts, nil
}

func resolveJSONPointer(value any, pointer string) (any, error) {
	parts, err := parseJSONPointer(pointer)
	if err != nil {
		return nil, err
	}
	current := value
	for _, part := range parts {
		switch node := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = node[part]
			if !ok {
				return nil, fmt.Errorf("path %q is absent", pointer)
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(node) {
				return nil, fmt.Errorf("path %q has an invalid array index", pointer)
			}
			current = node[index]
		default:
			return nil, fmt.Errorf("path %q traverses a scalar", pointer)
		}
	}
	return current, nil
}

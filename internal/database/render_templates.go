package database

import (
	"fmt"
	"strings"
	"text/template"
)

func renderTemplates(prefix string, tmpls ...*string) error {
	var params = inputParams{Prefix: prefix}

	for i := 0; i < len(tmpls); i++ {
		tmpl := tmpls[i]
		if tmpl == nil || *tmpl == "" {
			return fmt.Errorf("[es/postgres) Missing template")
		}

		t, err := template.New("sql").Parse(*tmpl)
		if err != nil {
			return fmt.Errorf("[es/postgres) Parsing template: %w", err)
		}

		var buf strings.Builder
		err = t.Execute(&buf, params)
		if err != nil {
			return fmt.Errorf("[es/postgres) Executing template: %w", err)
		}

		*tmpl = buf.String()
	}

	return nil
}

package catalog

import (
	"fmt"
	"io"
	"text/tabwriter"
)

// PrintCatalog writes a table-formatted summary to w.
func PrintCatalog(w io.Writer, cat *Catalog) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STREAM\tFIELD\tRAW TYPE\tNORM TYPE\tNULLABLE\tPK")
	fmt.Fprintln(tw, "──────\t─────\t────────\t─────────\t────────\t──")
	for _, s := range cat.Streams {
		for i, f := range s.Fields {
			stream := ""
			if i == 0 {
				stream = s.Namespace + "." + s.Name
			}
			nullable := "yes"
			if !f.Nullable {
				nullable = "no"
			}
			pk := ""
			if f.IsPrimary {
				pk = "✓"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
				stream, f.Name, f.RawType, f.NormType, nullable, pk)
		}
	}
	tw.Flush()
}

package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/xuri/excelize/v2"
)

func sql2excel(rows *sql.Rows, file string) {

	f := excelize.NewFile()
	index := f.NewSheet("Extract")

	f.SetActiveSheet(index)
	f.DeleteSheet("Sheet1")

	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	pretty := [][]string{cols}

	results := make([]interface{}, len(cols))
	for i := range results {
		v := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue("Extract", v, pretty[0][i])
		results[i] = new(interface{})
	}
	index = 1
	vv := `A1`
	for rows.Next() {
		index++
		if err := rows.Scan(results[:]...); err != nil {
			panic(err)
		}
		cur := make([]string, len(cols))
		for i := range results {

			val := *results[i].(*interface{})
			var str string

			vv = fmt.Sprintf("%c%v", 'A'+i, index)

			if val == nil {
				str = "NULL"
			} else {
				switch v := val.(type) {
				case []byte:
					str = string(v)
					tps, error := time.Parse("2006-01-02 15:04:05", str)
					if error != nil {
						integer, error := strconv.Atoi(str)
						if error != nil {
							float, error := strconv.ParseFloat(str, 64)
							if error != nil {
								date, error := time.Parse("2006-01-02", str)
								if error != nil {
									f.SetCellValue("Extract", vv, str)
								} else {
									f.SetCellValue("Extract", vv, date)
									exp := "yyyy-mm-dd"
									style, _ := f.NewStyle(&excelize.Style{CustomNumFmt: &exp})
									_ = f.SetCellStyle("Extract", vv, vv, style)
								}
							} else {
								f.SetCellValue("Extract", vv, float)
							}
						} else {
							f.SetCellValue("Extract", vv, integer)
						}
					} else {
						f.SetCellValue("Extract", vv, tps)
						exp := "yyyy-mm-dd hh:mm:ss"
						style, _ := f.NewStyle(&excelize.Style{CustomNumFmt: &exp})
						_ = f.SetCellStyle("Extract", vv, vv, style)
					}

				default:
					str = fmt.Sprintf("%v", v)
					f.SetCellValue("Extract", vv, v)
				}
			}

			cur[i] = str
		}
		pretty = append(pretty, cur)
	}

	_ = f.AddTable("Extract", "A1", vv, `{
		"table_name": "table",
		"table_style": "TableStyleMedium2",
		"show_first_column": false,
		"show_last_column": false,
		"show_row_stripes": true,
		"show_column_stripes": false
	}`)

	_ = f.SetPanes("Extract", `{
		"freeze": true,
		"split": true,
		"x_split": 0,
		"y_split": 1,
	}`)

	// Save spreadsheet by the given path.
	if err := f.SaveAs(file); err != nil {
		fmt.Println(err)
	}
}

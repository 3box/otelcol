// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquetexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/parquetexporter"

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/schema"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type parquetExporter struct {
	path string
	f    *os.File
	w    *file.Writer
	rg   file.BufferedRowGroupWriter

	names       *file.ByteArrayColumnChunkWriter
	metricTypes *file.ByteArrayColumnChunkWriter
	valueTypes  *file.ByteArrayColumnChunkWriter

	times *file.Int64ColumnChunkWriter

	valInts    *file.Int64ColumnChunkWriter
	valIntDL   int16
	valFloats  *file.Float64ColumnChunkWriter
	valFloatDL int16

	rescsKey *file.ByteArrayColumnChunkWriter
	rescsVal *file.ByteArrayColumnChunkWriter
	rescsDL  int16
	rescsRL  int16

	scopeKey *file.ByteArrayColumnChunkWriter
	scopeVal *file.ByteArrayColumnChunkWriter
	scopeDL  int16
	scopeRL  int16

	attrsKey *file.ByteArrayColumnChunkWriter
	attrsVal *file.ByteArrayColumnChunkWriter
	attrsDL  int16
	attrsRL  int16
}

func (e *parquetExporter) start(ctx context.Context, host component.Host) error {
	type MetricSchema struct {
		Name        string             `parquet:"fieldid=1"`
		MetricType  string             `parquet:"fieldid=2"`
		ValueType   string             `parquet:"fieldid=3"`
		Time        int64              `parquet:"fieldid=4,logical=timestamp,logical.unit=nanos"`
		ValueInt    *int64             `parquet:"fieldid=5"`
		ValueDouble *float64           `parquet:"fieldid=6"`
		Resources   *map[string]string `parquet:"fieldid=7,keylogical=String,valuelogical=String"`
		Scope       *map[string]string `parquet:"fieldid=8,keylogical=String,valuelogical=String"`
		Attributes  *map[string]string `parquet:"fieldid=9,keylogical=String,valuelogical=String"`
	}

	sc, err := schema.NewSchemaFromStruct(MetricSchema{})
	if err != nil {
		return err
	}

	//schema.PrintSchema(sc.Root(), os.Stdout, 2)
	if err := os.MkdirAll(e.path, 0755); err != nil {
		return err
	}
	e.f, err = os.Create(filepath.Join(e.path, "metrics-"+strconv.FormatInt(time.Now().UnixNano(), 10)+".parquet"))
	if err != nil {
		return err
	}

	e.w = file.NewParquetWriter(e.f, sc.Root())

	e.newRowGroup()

	return nil
}

func (e *parquetExporter) newRowGroup() error {
	// Close any existing group
	if err := e.closeRowGroup(); err != nil {
		return err
	}

	// Create a new row group
	e.rg = e.w.AppendBufferedRowGroup()

	namesCol, err := e.rg.Column(0)
	if err != nil {
		return err
	}
	e.names = namesCol.(*file.ByteArrayColumnChunkWriter)

	metricTypesCol, err := e.rg.Column(1)
	if err != nil {
		return err
	}
	e.metricTypes = metricTypesCol.(*file.ByteArrayColumnChunkWriter)

	valueTypesCol, err := e.rg.Column(2)
	if err != nil {
		return err
	}
	e.valueTypes = valueTypesCol.(*file.ByteArrayColumnChunkWriter)

	timesCol, err := e.rg.Column(3)
	if err != nil {
		return err
	}
	e.times = timesCol.(*file.Int64ColumnChunkWriter)

	valIntsCol, err := e.rg.Column(4)
	if err != nil {
		return nil
	}
	e.valInts = valIntsCol.(*file.Int64ColumnChunkWriter)
	e.valIntDL = e.valInts.Descr().MaxDefinitionLevel()

	valFloatsCol, err := e.rg.Column(5)
	if err != nil {
		return nil
	}
	e.valFloats = valFloatsCol.(*file.Float64ColumnChunkWriter)
	e.valFloatDL = e.valFloats.Descr().MaxDefinitionLevel()

	rescsKeyCol, err := e.rg.Column(6)
	if err != nil {
		return nil
	}
	e.rescsKey = rescsKeyCol.(*file.ByteArrayColumnChunkWriter)

	rescsValCol, err := e.rg.Column(7)
	if err != nil {
		return nil
	}
	e.rescsVal = rescsValCol.(*file.ByteArrayColumnChunkWriter)
	e.rescsDL = e.rescsKey.Descr().MaxDefinitionLevel()
	e.rescsRL = e.rescsKey.Descr().MaxRepetitionLevel()

	scopeKeyCol, err := e.rg.Column(8)
	if err != nil {
		return nil
	}
	e.scopeKey = scopeKeyCol.(*file.ByteArrayColumnChunkWriter)

	scopeValCol, err := e.rg.Column(9)
	if err != nil {
		return nil
	}
	e.scopeVal = scopeValCol.(*file.ByteArrayColumnChunkWriter)
	e.scopeDL = e.scopeKey.Descr().MaxDefinitionLevel()
	e.scopeRL = e.scopeKey.Descr().MaxRepetitionLevel()

	attrsKeyCol, err := e.rg.Column(10)
	if err != nil {
		return nil
	}
	e.attrsKey = attrsKeyCol.(*file.ByteArrayColumnChunkWriter)

	attrsValCol, err := e.rg.Column(11)
	if err != nil {
		return nil
	}
	e.attrsVal = attrsValCol.(*file.ByteArrayColumnChunkWriter)
	e.attrsDL = e.attrsKey.Descr().MaxDefinitionLevel()
	e.attrsRL = e.attrsKey.Descr().MaxRepetitionLevel()
	return nil
}

func (e *parquetExporter) closeRowGroup() error {
	// If we have an existing open row group, close it.
	if e.rg != nil {
		// Write out specific columns as dictionary columns
		e.names.WriteDictionaryPage()
		e.metricTypes.WriteDictionaryPage()
		e.valueTypes.WriteDictionaryPage()

		e.rescsKey.WriteDictionaryPage()
		e.rescsVal.WriteDictionaryPage()

		e.scopeKey.WriteDictionaryPage()
		e.scopeVal.WriteDictionaryPage()

		e.attrsKey.WriteDictionaryPage()
		e.attrsVal.WriteDictionaryPage()

		// Explicitly close column writers to catch specific errors
		if err := e.names.Close(); err != nil {
			return err
		}
		if err := e.metricTypes.Close(); err != nil {
			return err
		}
		if err := e.valueTypes.Close(); err != nil {
			return err
		}
		if err := e.times.Close(); err != nil {
			return err
		}
		if err := e.valInts.Close(); err != nil {
			return err
		}
		if err := e.valFloats.Close(); err != nil {
			return err
		}
		if err := e.rescsKey.Close(); err != nil {
			return err
		}
		if err := e.rescsVal.Close(); err != nil {
			return err
		}
		if err := e.scopeKey.Close(); err != nil {
			return err
		}
		if err := e.scopeVal.Close(); err != nil {
			return err
		}
		if err := e.attrsKey.Close(); err != nil {
			return err
		}
		if err := e.attrsVal.Close(); err != nil {
			return err
		}
		if err := e.rg.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (e *parquetExporter) shutdown(ctx context.Context) error {
	if err := e.closeRowGroup(); err != nil {
		return err
	}
	if err := e.w.Close(); err != nil {
		return err
	}
	//if err := e.f.Close(); err != nil {
	//	return err
	//}
	return nil
}

func (e *parquetExporter) consumeMetrics(ctx context.Context, ld pmetric.Metrics) error {
	namesData := make([]parquet.ByteArray, 0, 1024)
	metricTypesData := make([]parquet.ByteArray, 0, 1024)
	valueTypesData := make([]parquet.ByteArray, 0, 1024)

	timesData := make([]int64, 0, 1024)

	valIntsData := make([]int64, 0, 1024)
	valIntDef := make([]int16, 0, 1024)

	valFloatsData := make([]float64, 0, 1024)
	valFloatDef := make([]int16, 0, 1024)

	rescsKeyData := make([]parquet.ByteArray, 0, 1024)
	rescsValData := make([]parquet.ByteArray, 0, 1024)
	rescsDef := make([]int16, 0, 1024)
	rescsRep := make([]int16, 0, 1024)

	scopeKeyData := make([]parquet.ByteArray, 0, 1024)
	scopeValData := make([]parquet.ByteArray, 0, 1024)
	scopeDef := make([]int16, 0, 1024)
	scopeRep := make([]int16, 0, 1024)

	attrsKeyData := make([]parquet.ByteArray, 0, 1024)
	attrsValData := make([]parquet.ByteArray, 0, 1024)
	attrsDef := make([]int16, 0, 1024)
	attrsRep := make([]int16, 0, 1024)

	count := 0
	appendPoint := func(name string, typ string, resource pcommon.Resource, scope pcommon.InstrumentationScope, dataPoint pmetric.NumberDataPoint) {
		count += 1

		namesData = append(namesData, parquet.ByteArray(name))
		metricTypesData = append(metricTypesData, parquet.ByteArray(typ))

		time := dataPoint.Timestamp().AsTime().UnixNano()
		timesData = append(timesData, time)

		switch dataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			valueTypesData = append(valueTypesData, parquet.ByteArray("int"))
			valIntsData = append(valIntsData, dataPoint.IntValue())
			valIntDef = append(valIntDef, e.valIntDL)

			valFloatDef = append(valFloatDef, e.valFloatDL-1)
		case pmetric.NumberDataPointValueTypeDouble:
			valueTypesData = append(valueTypesData, parquet.ByteArray("double"))
			valFloatsData = append(valFloatsData, dataPoint.DoubleValue())
			valFloatDef = append(valFloatDef, e.valFloatDL)

			valIntDef = append(valIntDef, e.valIntDL-1)
		}

		rescsKeyData, rescsValData, rescsDef, rescsRep = appendAttributes(resource.Attributes(), rescsKeyData, rescsValData, rescsDef, rescsRep, e.rescsDL, e.rescsRL)
		scopeKeyData, scopeValData, scopeDef, scopeRep = appendAttributes(scope.Attributes(), scopeKeyData, scopeValData, scopeDef, scopeRep, e.scopeDL, e.scopeRL)
		attrsKeyData, attrsValData, attrsDef, attrsRep = appendAttributes(dataPoint.Attributes(), attrsKeyData, attrsValData, attrsDef, attrsRep, e.attrsDL, e.attrsRL)

	}

	for i := 0; i < ld.ResourceMetrics().Len(); i++ {
		resourceMetrics := ld.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			ilMetrics := resourceMetrics.ScopeMetrics().At(j)
			l := ilMetrics.Metrics().Len()
			for k := 0; k < l; k++ {
				metric := ilMetrics.Metrics().At(k)

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					for i := 0; i < metric.Gauge().DataPoints().Len(); i++ {
						dataPoint := metric.Gauge().DataPoints().At(i)
						appendPoint(metric.Name(), "gauge", resourceMetrics.Resource(), ilMetrics.Scope(), dataPoint)
					}
				case pmetric.MetricTypeSum:
					for i := 0; i < metric.Sum().DataPoints().Len(); i++ {
						dataPoint := metric.Sum().DataPoints().At(i)
						appendPoint(metric.Name(), "sum", resourceMetrics.Resource(), ilMetrics.Scope(), dataPoint)
					}
				default:
					log.Printf("unsupportted metric type %q", metric.Type())
				}
			}

		}
	}

	e.names.WriteBatch(namesData, nil, nil)

	e.metricTypes.WriteBatch(metricTypesData, nil, nil)
	e.valueTypes.WriteBatch(valueTypesData, nil, nil)

	e.times.WriteBatch(timesData, nil, nil)

	e.valInts.WriteBatch(valIntsData, valIntDef, nil)
	e.valFloats.WriteBatch(valFloatsData, valFloatDef, nil)

	e.rescsKey.WriteBatch(rescsKeyData, rescsDef, rescsRep)
	e.rescsVal.WriteBatch(rescsValData, rescsDef, rescsRep)

	e.scopeKey.WriteBatch(scopeKeyData, scopeDef, scopeRep)
	e.scopeVal.WriteBatch(scopeValData, scopeDef, scopeRep)

	e.attrsKey.WriteBatch(attrsKeyData, attrsDef, attrsRep)
	e.attrsVal.WriteBatch(attrsValData, attrsDef, attrsRep)

	log.Printf("batch had %d points", count)
	if count, err := e.rg.NumRows(); err != nil {
		return err
	} else if int64(count) > parquet.DefaultWriteBatchSize {
		e.newRowGroup()
	}

	return nil
}

func appendAttributes(attrs pcommon.Map, key, val []parquet.ByteArray, def, rep []int16, dl, rl int16) ([]parquet.ByteArray, []parquet.ByteArray, []int16, []int16) {
	start := len(rep)
	count := 0
	attrs.Range(func(k string, v pcommon.Value) bool {
		count += 1
		key = append(key, parquet.ByteArray(k))
		val = append(val, parquet.ByteArray(v.AsString()))
		def = append(def, dl)
		rep = append(rep, rl)
		return true
	})
	if count > 0 {
		// We have attrs, mark the start
		rep[start] = 0
	} else {
		// No attributes mark as undefined
		def = append(def, 0)
	}
	return key, val, def, rep
}

func (e *parquetExporter) consumeTraces(ctx context.Context, ld ptrace.Traces) error {
	return nil
}

func (e *parquetExporter) consumeLogs(ctx context.Context, ld plog.Logs) error {
	return nil
}

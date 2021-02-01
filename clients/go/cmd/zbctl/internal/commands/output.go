// Copyright © 2018 Camunda Services GmbH (info@camunda.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
)

type ProtoMessage proto.Message
type Command cobra.Command

type Printer interface {
	print(message Printable) error
}
type JSONPrinter struct {
}
type HumanPrinter struct {
}

func findPrinter() (Printer, error) {
	printer := outputMap[outputFlag]
	if printer == nil {
		return nil, fmt.Errorf("cannot find proper printer for %s output", outputFlag)
	} else {
		return printer, nil
	}
}

const humanOutput = "human"
const jsonOutput = "json"

var (
	outputFlag string
	outputMap  = map[string]Printer{humanOutput: &HumanPrinter{}, jsonOutput: &JSONPrinter{}}
)

func (jsonPrinter *JSONPrinter) print(message Printable) error {
	return printJSON(message.protoMessage())
}

func (humanPrinter *HumanPrinter) print(message Printable) error {
	message.print()
	return nil
}

type Printable interface {
	protoMessage() ProtoMessage
	print()
}

func addOutputFlag(c *cobra.Command) {
	c.Flags().StringVarP(
		&outputFlag,
		"output",
		"o",
		humanOutput,
		"Specify output format. Default is human readable. Possible Values: human, json",
	)
}
